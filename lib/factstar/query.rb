require 'msp/reports2/schema'

module Factstar

  class Query

    OPTION_KEYS = [:fact, :select, :aggregates, :group_by, :include_all_values,
      :filters, :may_promote_range_filters, :second_aggregates,
      :second_group_by, :order, :limit, :distribution]

    # Main constructor point for queries
    def self.new(*args)
      return super unless self == Query

      schema, raw_opts = *args
      options = {}
      OPTION_KEYS.each {|key| v = raw_opts[key.to_s] || raw_opts[key]; options[key] = v if v}

      fact = schema[options[:fact]]
      raise InvalidQueryParams, "fact #{options[:fact]} not found" unless fact.is_a?(Type::Fact)

      query_class = if options[:second_aggregates]
        Query::SecondAggregate
      else
        Query::Aggregate
      end

      if options[:distribution]
        query_class.new_with_distribution(fact, options)
      else
        query_class.new(fact, options)
      end
    end

    include Enumerable

    def db
      raise NotImplementedError
    end

    # Runs the query, doing any necessary setup and ensuring to do any necessary cleanup afterwards.
    #
    # Should yield an enumerable yielding Query::Row instances; note this runs off
    # the dataset's .each method so consumes the underlying sql resultset one row
    # at a time.
    #
    # Superclass implementation doesn't do any special setup / teardown
    def execute
      yield ResultEnumerator.new(self)
    end

    def dataset
      raise NotImplementedError
    end

    # an array of objects responding to:
    #  aliased_column_expression
    #  column_alias
    #  value_type
    def columns
      raise NotImplementedError
    end

    def columns_by_name
      @columns_by_name ||= Hash[@columns.map {|c| [c.to_s, c]}]
    end

    def column_string_array
      columns.map {|c| c.to_s}
    end

    # CSV anyone?
    def column_and_rows_string_arrays
      execute do |rows|
        [column_string_array] + rows.map {|row| row.to_string_array}
      end
    end

    # for jsoning
    def to_data
      execute do |rows|
        {
          :columns => columns.map {|col| col.to_data},
          :rows => rows.map {|row| row.to_a}
        }
      end
    end

    # Create a distribution query from this one. will only work with single-numeric-column queries.
    def distribution_query
      Query::Distribution.new(self)
    end

    private
      # Tries to find a column from the given list - will take either a column object,
      # or a string / symbol for the column's string name (eg 'count' or 'stream.track')
      # or an array of string/symbols as the name for a dimension path column [:stream, :track]
      def find_column(column, columns=self.columns, create_dimpaths_from=nil)
        case column
        when Query::Column
          column if columns.include?(column)
        when Array
          if create_dimpaths_from
            Query::Column::DimensionPath.new(create_dimpaths_from, column)
          else
            columns.find {|col| col.is_a?(Query::Column::DimensionPath) && col.names == column}
          end
        else
          columns.find {|col| col.to_s == column.to_s}
        end
      end

      def add_order_and_limit(dataset, order_column_name, columns, order_dir=nil, limit=nil, graceful_fail=false)
        order_col = find_column(order_column_name, columns) or if graceful_fail
          return dataset
        else
          raise InvalidQueryParams, "Requested order column not found amongst the available columns"
        end

        order_dir ||= "asc"
        raise InvalidQueryParams, "Bad order direction" unless ["asc", "desc"].include?(order_dir.to_s)
        dataset = dataset.order(order_col.column_alias.send(order_dir))

        limit ? dataset.limit(limit) : dataset
      end
  end
end
