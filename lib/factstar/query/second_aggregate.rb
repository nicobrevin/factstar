module Factstar
  # A query which groups a second time by an aggregrate resulting from an inner aggregate query.
  # This is a little bit more restrictive than it needs to be when it comes to what you can select and group by,
  # to avoid the need for more complex logic to determine column dependencies in the presence of aggregate columns
  # alongside non-aggregates
  class Query::SecondAggregate < Query::Aggregate
    attr_reader :second_aggregates, :second_group_by

    def parse_args
      @second_group_by = [*@options[:second_group_by] || []]
      make_dimpath_columns!(@second_group_by)
      if Query::Column::DimensionPath.pairwise_dependencies?(*@second_group_by)
        # this would be redundant but harmless in simple cases, but being strict about excluding redundant
        # group-by columns makes it makes it easier to reason about correctness of more complex queries
        raise InvalidQueryParams, "Pairwise dependencies are not allowed between dimension paths specified for second_group_by"
      end

      @options[:select] ||= @second_group_by

      super

      @second_aggregates = [*@options[:second_aggregates] || []]
      make_aggregate_funcs_from_args!(@second_aggregates, @aggregates)

      @second_group_by.each do |sgb|
        unless sgb.is_dependent_on_any_of?(*@group_by)
          raise InvalidQueryParams, "second_group_by path #{sgb} was not dependent on any of the group_by paths"
        end
      end

      @select.each do |dimpath|
        unless dimpath.is_dependent_on_any_of?(*@second_group_by)
          raise InvalidQueryParams, "path #{dimpath} requested to select for output, was not dependent on any of the second_group_by paths"
        end
        # were this postgres, we would need to wrap any selected non-aggregates that aren't exactly equal to one of the
        # group-by columns, but are dependent on them, with a first() function. mysql does this implicitly.
      end
    end

    def init_columns_and_dataset(temp_table_context)
      paths_referenced_by_second_aggregates = @second_aggregates.map {|agg| agg.arguments}.flatten.grep(Query::Column::DimensionPath)
      paths_referenced_by_second_aggregates.each do |path|
        raise InvalidQueryParams, "Path #{path} referenced by second_aggregate not dependent on any of the group_by columns" unless path.is_dependent_on_any_of?(*@group_by)
      end

      paths_to_select_for_first_aggregate = paths_referenced_by_second_aggregates | @select | @second_group_by
      first_aggregate_columns, first_aggregate_dataset = make_aggregate_query_dataset(paths_to_select_for_first_aggregate, @aggregates, false, temp_table_context)

      @columns = @select.map {|c| c.of_subquery(:__first_aggregate)} + @second_aggregates.map {|c| c.map_args {|arg| arg.of_subquery(:__first_aggregate)}}
      @dataset = db[first_aggregate_dataset.as(:__first_aggregate)]
      @dataset = dataset.select(*@columns.map {|c| c.aliased_column_expression})
      @dataset = dataset.group_by(*@second_group_by.map {|c| c.of_subquery(:__first_aggregate).column_expression}) unless @second_group_by.empty?

      @dataset = add_order_and_limit(dataset, @order_column_name, @columns, @order_dir, @limit) if @order_column_name
    end
  end
end
