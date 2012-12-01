module Factstar

  # Superclass just represents a plain column of a query which doesn't need to be qualified or aliased.
  # There are subclasses for aggregate functions, columns from a subquery aliased for use in an outer query,
  # and for 'dimension path' columns which are columns corresponding to dimensions in the reporting schema.
  class Query::Column
    def initialize(column_alias, value_type, column_expression=column_alias)
      @column_alias = column_alias
      @value_type = if value_type.is_a?(Type::Value)
        value_type
      else
        VALUE_TYPES[value_type] or raise "Value type not found"
      end
      @column_expression = column_expression
    end

    def adapter
      MSP::Reports2.adapter
    end

    def to_data
      {
        :name       => to_s,
        :value_type => value_type.name,
        :null       => null?
      }
    end

    def dup_with_column_expression(column_expression=@column_expression)
      self.class.new(@column_alias, @value_type, column_expression)
    end

    def column_alias
      @column_alias
    end

    def value_type
      @value_type
    end

    def column_expression
      @column_expression
    end

    def aliased_column_expression
      @aliased_column_expression ||= column_expression.as(column_alias)
    end

    def to_s
      column_alias.to_s
    end

    def inspect
      "#<#{self.class}: #{column_alias}>"
    end

    def ==(other)
      other.is_a?(Query::Column) && other.column_alias == column_alias
    end

    alias :eql? :==

    def hash
      column_alias.hash
    end

    def of_subquery(subquery_name)
      dup_with_column_expression(column_alias.qualify(subquery_name))
    end

    # may values of this column be null/nil?
    def null?; false; end
  end

end
