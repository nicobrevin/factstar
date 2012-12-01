module Factstar
  class Query::Aggregate < Query
    attr_reader :fact, :select, :aggregates, :group_by, :dataset, :columns

    def db
      @fact.schema.database
    end

    alias :execute_without_context :execute

    # Our execute needs to wrap in a temporary table context.
    # At present we're kinda assuming you only run execute once per query instance.
    def execute(&block)
      TempTableContext.with_context(db) {|context| execute_in_context(context, &block)}
    end

    def execute_in_context(context, &block)
      init_columns_and_dataset(context)
      execute_without_context(&block)
    end

    def initialize(fact, options={}, &block)
      @fact = fact
      @options = options
      parse_args
    end

    private

    def parse_args
      @aggregates = [*@options[:aggregates] || [Query::Column::AggregateFunc::Count.new([], :count)]]
      raise InvalidQueryParams, "You need to specify at least one aggregate" if @aggregates.empty?
      make_aggregate_funcs_from_args!(@aggregates)

      @group_by   = [*@options[:group_by]   || []]
      make_dimpath_columns!(@group_by)
      if Query::Column::DimensionPath.pairwise_dependencies?(*@group_by)
        # this would be redundant but harmless in simple cases, but being strict about excluding redundant
        # group-by columns makes it makes it easier to reason about correctness of more complex queries
        raise InvalidQueryParams, "Pairwise dependencies are not allowed between dimension paths specified for group_by"
      end

      @select     = [*@options[:select]     || @group_by]
      begin
        select = @select.clone
        make_dimpath_columns!(@select)
      rescue ArgumentError
        raise ArgumentError, "Couldn't create select from: #{select.inspect}"
      end

      @filters    = [*@options[:filters]    || []]
      make_filters_from_dimpath_columns!(@filters)

      @order_column_name, @order_dir = @options[:order]
      @limit = @options[:limit]

      # This is a facility to do some outer joins in order to ensure that all possible values of the grouped-by dimensions
      # are included in the result. If it's used, we'll wrap the main aggregate query as a subquery, and then do a
      # right outer join to all the possible values for the grouped-by columns, supplying default values for the aggregate
      # function columns.
      @include_all_values = @options[:include_all_values] || false

      @select.each do |dimpath|
        unless dimpath.is_dependent_on_any_of?(*@group_by)
          raise InvalidQueryParams, "path #{dimpath} requested to select for output, was not dependent on any of the group_by columns"
        end
        # were this postgres, we would need to wrap any selected non-aggregates that aren't exactly equal to one of the
        # group-by columns, but are dependent on them, with a first() function. mysql does this implicitly.
      end
    end

    def paths_in_aggregates(aggregates)
      aggregates.map {|aggregate| aggregate.arguments}.flatten
    end

    def paths_in_filters
      @filters.map {|filter| filter.arguments}.flatten
    end

    def paths_to_be_accessible_for_inner_query(columns_to_select, aggregates)
      # make sure we have all the joins we need to refer to the columns used in aggregates, non-aggregate columns to be selected,
      # filters and the group-by columns
      (paths_in_aggregates(aggregates) | paths_in_filters | columns_to_select | @group_by).uniq
    end

    # paths which a particular filter condition forces to be non-null (most filters do), also force their ancestors to be
    # non-null. We can use this list later to avoid the need for some left joins.
    def paths_forced_not_null_by_filters
      @filters.map {|filter| filter.arguments_forced_not_null.map {|dp| dp.ancestors}}.flatten.uniq
    end

    def make_inner_query_dataset(columns_to_select, aggregates, add_order_clause=true)
      # calculate the tree of joins required to expose the paths_to_be_accessible_for_inner_query:
      column_paths_to_expressions, table_with_joins = @fact.columns_and_table_with_joins_for_dimension_paths(
        paths_to_be_accessible_for_inner_query(columns_to_select, aggregates).map {|dp| dp.names}, [@fact.name],
        paths_forced_not_null_by_filters.map {|dp| dp.names}
      )
      # make a dataset from them:
      dataset = Utils.make_dataset(db, table_with_joins)
      # apply filters:
      @filters.each {|f| dataset.filter!(f.sequel_expression)}
      # select appropriate columns:
      columns_of_inner_query = (columns_to_select + aggregates)
      dataset.select!(*columns_of_inner_query.map {|c| c.aliased_column_expression})
      # apply group_by:
      # If there are no group_by columns, it's just a plain aggregate query returning a single row (select count(*) from foo)
      # In this case no select columns will have been allowed, since there are no group_by columns for them
      # to have depended on.
      unless @group_by.empty?
        dataset.group_by!(
          # apply group_bys
          *@group_by.map {|dimpath| dimpath.column_expression}
        )
      end
      # add order clause
      if add_order_clause && @order_column_name
        dataset = add_order_and_limit(dataset, @order_column_name, columns_of_inner_query, @order_dir, @limit, add_order_clause == :if_you_can)
      end
      # done
      [columns_of_inner_query, dataset]
    end

    def init_columns_and_dataset(temp_table_context)
      @columns, @dataset = make_aggregate_query_dataset(@select, @aggregates, true, temp_table_context)
    end

    def make_aggregate_query_dataset(columns_to_select, aggregates, add_order_clause, temp_table_context)
      if @include_all_values
        make_include_all_values_outer_query_dataset(columns_to_select, aggregates, add_order_clause, temp_table_context)
      else
        make_inner_query_dataset(columns_to_select, aggregates, add_order_clause)
      end
    end

    def make_include_all_values_outer_query_dataset(columns_to_select, aggregates, add_order_clause, temp_table_context)
      # we need a mapping between columns of the inner query and the outer query.
      # for paths which were dependent on grouped-by paths, we will add mappings to columns of the
      # tables we outer join onto in the outer subquery. Other columns will default to mapping directly to columns
      # of the subquery.
      inner_to_outer_columns_mapping = Hash.new {|h,col| col.of_subquery(:__inner)}

      # If we're wrapping in an outer query and doing left joins to all the group_by columns to include_all_values,
      # then we only need to select the group_by values themselves in the inner query; any other selected values will
      # be selected from the tables joined to in the outer query.
      #
      # We also only try to add an order clause to the inner query if there's a limit specified.
      # Because in this case the same order and limit are applied to both queries, there's no risk of it affecting the
      # results and (because of the limit) it will save some work.
      # (You might think there could be an issue with left-joining the possible values onto only the
      # top n results, whereby possible values which weren't in the top 10 would have their aggregates erroneously listed as
      # zero / null. These items will never appear because the outer query has the same order and limit on it, and if the item
      # didn't make the top 10 of the inner query, adding some more values in the outer query is only gonna make it further
      # from being in the top 10.)
      inner_query_columns, inner_query_dataset = make_inner_query_dataset(@group_by, aggregates, add_order_clause && !@limit.nil? && :if_you_can)

      left_join, right_join, join_conditions = true, false, {}

      # figure out the tables (with further joins) which we need to left join onto to get these paths
      tables_with_joins = @group_by.map do |group_by_path|

        # which dimension paths do we need to join for within this include_all_values bit of the query?
        # we need all the dimension paths which the user originally wanted to select, plus any dimension paths
        # referenced in filters, provided these are dependent on the grouped-by path in question (for which we're
        # including all values).
        dependent_paths = (paths_in_filters | columns_to_select).select {|path| path.is_dependent_on?(group_by_path)}

        # if the type is infinite and we want to include all its values, we need a range filter to use to restrict
        # the range of primary key values
        range = if group_by_path.type.infinite?
          range_from_path_and_filters(group_by_path, @filters, @options[:may_promote_range_filters]) or raise InvalidQueryParams, \
             "include_all_values was specified with a grouped-by path (#{group_by_path}) whose type is infinite, but no range
              filter was given on that path, and additionally either :may_promote_range_filters was not specified,
              or we were not able to promote any range filters supplied on parent paths"
        end

        relative_dependent_paths = dependent_paths.map {|p| p.relative_to(group_by_path).names}
        relative_paths_forced_not_null = paths_forced_not_null_by_filters.map {|p| rel = p.relative_to(group_by_path) and rel.names}.compact

        col_exprs_by_relative_path, table_with_joins = group_by_path.type.
          columns_and_table_with_joins_for_dimension_paths(relative_dependent_paths, group_by_path.fact_and_names, relative_paths_forced_not_null, range, temp_table_context)

        # add to a mapping between columns of the original query and columns to select (or use in filters that need re-applying)
        # in the outer query:
        dependent_paths.each do |dep_path|
          rel_path = dep_path.relative_to(group_by_path).names
          col_expr = col_exprs_by_relative_path[rel_path]
          # we need to map the DimensionPath column to a new version with a different column_expression based on that
          # returned from the columns_and_table_with_joins_for_dimension_paths call for the right-joined table:
          inner_to_outer_columns_mapping[dep_path] = dep_path.dup_with_column_expression(col_expr)
        end

        # If any of the grouped-by paths might be null, we need a full outer join, since rows may be missing on either side
        # (some possible values for the grouped-by columns may be missing on the right, and null values for (some of) the grouped-by
        #  columns may be missing on the left)
        right_join = true if group_by_path.null? && !paths_forced_not_null_by_filters.include?(group_by_path)

        join_conditions[Utils.qualified_primary_key(table_with_joins)] = group_by_path.column_alias.qualify(:__inner)

        # aggregate values need to get mapped via special logic which replaces null values corresponding to 'include all values' rows
        # that weren't present in the original aggregation, with the identity of the aggregate operator (eg a count of zero)
        aggregates.each do |aggregate|
          inner_to_outer_columns_mapping[aggregate] = aggregate.of_subquery_mapping_null_to_identity(:__inner)
        end

        table_with_joins
      end

      # Make a new dataset out of the cartesian product of the 'all values' tables for the grouped-by columns
      dataset = Utils.make_dataset(db, *tables_with_joins)

      # Re-apply any filters which are dependent on grouped-by paths, to the columns as mapped to this new outer query
      @filters.each do |filter|
        if filter.is_dependent_on?(*@group_by)
          unless filter.is_dependent_only_on?(*@group_by)
            raise InvalidQueryParams, "We don't yet handle include_all_values with filters which reference both columns dependent on the group_by paths and columns not"
          end
          dataset = dataset.filter(filter.map_args {|arg| inner_to_outer_columns_mapping[arg]}.sequel_expression)
        end
      end

      # Join the possible values to the inner query which containts the aggregates obtained for
      # grouped-by columns where rows for them were present
      dataset = dataset.join_table(join_type_for(left_join, right_join), inner_query_dataset.as(:__inner), join_conditions)

      # map the columns we originally wanted to select to columns from the outer query (based on the mapping):
      outer_select_cols = (columns_to_select + aggregates).map {|col| inner_to_outer_columns_mapping[col]}
      dataset = dataset.select(*outer_select_cols.map {|col| col.aliased_column_expression})

      # Now add the order/limit to the outer query (after having possibly added it to the inner query too, see earlier):
      dataset = add_order_and_limit(dataset, @order_column_name, outer_select_cols, @order_dir, @limit) if add_order_clause && @order_column_name

      [outer_select_cols, dataset]
    end

    # Takes a more limited set of options:
    # :group_by
    # :include_all_values
    # :aggregate - the single numeric aggregate whose distribution you want - defaults to count
    def self.new_with_distribution(fact, options={})
      options[:select] ||= []
      new(fact, options).distribution_query
    end

    private

    def make_dimpath_columns!(array)
      array.map! do |x|
        if x.is_a?(Query::Column::DimensionPath)
          x
        else
          Query::Column::DimensionPath.new(@fact, x)
        end
      end
    end

    def make_filters_from_dimpath_columns!(array)
      array.map! do |x|
        if x.is_a?(Query::Filter)
          x
        else
          begin
            Query::Filter.new_from_fact_and_external_args(@fact, *x)
          rescue InvalidQueryParams
            raise InvalidQueryParams, "Unable to create filter from fact #{@fact.name} #{[*x].inspect}: #{$!.to_s}"
          end
        end
      end
    end

    def make_aggregate_funcs_from_args!(array, columns=[])
      array.map! do |x|
        if x.is_a?(Query::Column::AggregateFunc)
          x
        else
          column_alias, type, *args = x
          Query::Column::AggregateFunc.new_from_fact_and_args_with_paths(@fact, column_alias.to_sym, type, args, columns)
        end
      end
    end

    # Tries to get a range which the given path is restricted by, based on the given filters.
    # looks for a range filter on the path, or if may_promote is allowed, also looks for an ancestor path
    # which can be promoted to be a range filter on the path. Then returns the range of that filter.
    def range_from_path_and_filters(path, filters, may_promote=false)
      range_filters = filters.grep(Query::Filter::Range)
      (may_promote ? path.ancestors : [path]).each do |p|
        filter = range_filters.find {|filter| filter.column == p}
        filter and promoted = filter.promote_to(path) and return promoted.range
      end
      nil
    end

    def join_type_for(left_join, right_join)
      case [left_join, right_join]
      when [true, true]
        if db.is_a?(Sequel::MySQL::DatabaseMethods)
          raise InvalidQueryParams, "include_all_values not supported where one of the grouped-by paths is nullable. Try specify a not_null
                 filter or another filter on the path which implies not null (because of lack of full outer join support from mysql)"
        end
        :full_outer
      when [true, false] then :left_outer
      when [false, true] then :right_outer
      when [false, false] then :inner
      end
    end
  end
end
