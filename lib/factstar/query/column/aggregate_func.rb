module Factstar

  class Query::Column::AggregateFunc < Query::Column
    # A convenience which helps construct an aggregate function relative to a particular fact
    # where dimension path columns are given as arrays of symbols rather than
    # DimensionPath objects. Can also give it some columns to recognise by their name
    # when translating args.
    def self.new_from_fact_and_args_with_paths(fact, column_alias, type, args, columns=[])
      klass = const_get(type.to_s.camelize)
      args.map! do |arg|
        case arg
        when Symbol,String then columns.find {|col| col.to_s == arg.to_s} or raise InvalidQueryParams, "Couldn't find column #{arg}"
        when Array then Query::Column::DimensionPath.new(fact, arg)
        else arg
        end
      end
      klass.new(args, column_alias)
    end

    attr_reader :arguments, :column_alias

    def initialize(arguments, column_alias, column_expression=nil)
      @arguments = arguments
      @column_alias = column_alias
      @column_expression = column_expression
    end

    def column_expression
      @column_expression or raise NotImplementedError
    end

    def dup_with_column_expression(column_expression=@column_expression)
      self.class.new(@arguments, @column_alias, column_expression)
    end

    def aliased_column_expression
      column_expression.as(column_alias)
    end

    def to_s
      @column_alias.to_s
    end

    # should return a new equivalent aggregate fn with the argument column(s) mapped via the block given.
    def map_args(&block)
      self.class.new(@arguments.map(&block), @column_alias)
    end

    # What does this function return when it aggregates over zero rows?
    # defaults to nil (which in SQL also stands in for +/-infinity and NaN),
    # however some (like sum and count) can have a meaningful value over 0 rows
    def identity_value
      nil
    end

    def null?
      identity_value.nil?
    end

    def of_subquery_mapping_null_to_identity(subquery_name)
      new_col_expr = column_alias.qualify(subquery_name)
      new_col_expr = adapter.ifnull(new_col_expr, identity_value) unless identity_value.nil?
      dup_with_column_expression(new_col_expr)
    end

    class Count < Query::Column::AggregateFunc
      def column_expression
        @column_expression ||= :count.sql_function("*".lit)
      end

      def value_type
        VALUE_TYPES[:integer]
      end

      def identity_value; 0; end
    end

    class Sum < Query::Column::AggregateFunc
      def column_expression
        @column_expression ||= :sum.sql_function(@arguments.first.column_expression)
      end

      def value_type
        @arguments.first.value_type
      end

      def identity_value; 0; end
    end

    class Average < Query::Column::AggregateFunc
      def column_expression
        @column_expression ||= :avg.sql_function(@arguments.first.column_expression)
      end

      def value_type
        VALUE_TYPES[:float]
      end
    end

    class StdDev < Query::Column::AggregateFunc
      def column_expression
        @column_expression ||= :stddev.sql_function(@arguments.first.column_expression)
      end

      def value_type
        VALUE_TYPES[:float]
      end
    end

    class Max < Query::Column::AggregateFunc
      def column_expression
        @column_expression ||= :max.sql_function(@arguments.first.column_expression)
      end

      def value_type
        @arguments.first.value_type
      end
    end

    class Min < Query::Column::AggregateFunc
      def column_expression
        @column_expression ||= :min.sql_function(@arguments.first.column_expression)
      end

      def value_type
        @arguments.first.value_type
      end
    end

    class WeightedSum < Query::Column::AggregateFunc
      def column_expression
        @column_expression ||= :sum.sql_function(@arguments[0].column_expression * @arguments[1].column_expression)
      end

      def value_type
        VALUE_TYPES[:float]
      end

      def identity_value; 0; end
    end

  end
end
