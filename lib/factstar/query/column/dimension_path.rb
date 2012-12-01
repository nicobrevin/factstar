module Factstar

  # A column corresponding to a dimension path in the snowflake reporting schema, that is a fact together with a path
  # of dimensions going outward from that fact, eg stream.track.release.title.
  class Query::Column::DimensionPath < Query::Column
    attr_reader :fact, :names

    def initialize(fact, names, column_expression=nil)
      @fact = fact
      @names = names or raise ArgumentError, "names can not be nil"
      # you can pass this in based off a previous call to columns_and_table_with_joins_for_dimension_paths with
      # column_and_joins_for_dimension_path if you want, whether to save work or because you needed to specify a
      # particular table alias with those calls.
      # If you don't pass it in we'll call column_and_joins_for_dimension_path ourselves to get this, without any
      # table alias.
      @column_expression = column_expression
    end

    def to_data
      result = super
      result[:dimension_path] = [@fact.name, *@names]
      media_type = self.type.media_type and result[:value_media_type] = media_type
      result
    end

    def dup_with_column_expression(column_expression=@column_expression)
      self.class.new(@fact, @names, column_expression)
    end

    def to_a
      @names
    end

    def column_alias
      @column_alias ||= to_s.to_sym
    end

    def to_s
      [@fact.name, *@names].join('.')
    end

    def fact_and_names
      [@fact.name, *@names]
    end

    # When a query constructed from this base fact with this path of dimension names, what is the column name,
    # qualified with the appropriate aliased table name from the FROM clause, for this dimension path?
    def column_expression
      @column_expression ||= @fact.column_and_joins_for_dimension_path(@names).first
    end

    def ==(other)
      other.is_a?(DimensionPath) && @fact.name == other.fact.name && @names == other.names
    end

    alias :eql? :==

    def hash
      [@fact.name, *@names].hash
    end

    def is_dependent_on?(other)
      return false unless @fact.name == other.fact.name

      # "@names.start_with?(other.names)"
      other.names.each_with_index {|name, i| return false unless @names[i] == name}
      return true
    end

    # Returns a 'rebased' DimensionPath for this path relative to the given path (which this path should be dependent on).
    # Will be based at the type of that path.
    def relative_to(other, column_expression=nil)
      return unless is_dependent_on?(other)
      DimensionPath.new(other.type, @names[other.names.length..-1], column_expression)
    end

    def parent
      DimensionPath.new(@fact, @names[0...-1]) unless @names.empty?
    end

    def ancestors
      (0...@names.length).map {|i| DimensionPath.new(@fact, @names[0..i])}
    end

    # Do any nontrivial pairwise dependencies exist between the given dimension paths?
    # (certain queries require no such dependencies for certain argument lists, eg group by columns.
    #  partly just to disallow redundant group by columns, partly to stop the include_all_values right
    #  join logic getting its knickers in a twist)
    def self.pairwise_dependencies?(*paths)
      # could easily beat O(n^2), but it's hardly the bottleneck...
      paths.any? {|p| paths.any? {|q| p != q && p.is_dependent_on?(q)}}
    end

    def is_dependent_on_any_of?(*paths)
      paths.any? {|path| is_dependent_on?(path)}
    end

    def self.any_dependencies?(paths1, paths2)
      paths1.any? {|p| p.is_dependent_on_any_of?(*paths2)}
    end

    def value_type
      @value_type ||= type.value_type
    end

    def type
       @fact.type_by_path(*@names)
    end

    # A dimension path may be null if any of the dimensions along the way may be.
    def null?
      type = @fact
      @names.each do |name|
        dim = type[name]
        return true if dim.null?
        type = dim.type
      end
      return false
    end
  end
end
