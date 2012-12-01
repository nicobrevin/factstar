
# A dimension is effectively a column of a fact type or of a dimension type.
#
# Has a name and a type whose primary dimension it references (this may be
# a value type, meaning it's just a straightforward value of that type, or a
# dimension type, meaning it references that dimension type together with the
# dependent dimensions that type declares)
#
# Dimensions may only reference value or dimension types, since fact types have
# multi-column primary keys.
#
# See docs on the attributes / methods here for options which may be used for
# particular dimensions of a type.
class Factstar::Schema::Dimension

  attr_reader :name, :type, :options

  DEFAULT_OPTIONS = {
    :on_table => true,
    :filterable => false,
    :group_by => true,
    :aggregate_after_grouping => true,
    :denormalized => nil,
    :dependency_respects_ordering => nil,
    :null => false
  }

  def initialize(name, type, options={}, &block)
    @name = name
    @type = type
    options = DEFAULT_OPTIONS.merge(options)

    @on_table = options[:on_table]
    raise InvalidSchemaDefinition, "Dimensions of fact types must always be on_table" if @type.is_a?(Type::Fact) && !@on_table

    @column_name = options[:column_name] ||
      (type.is_a?(Type::Dimension) &&
      type.primary_dimension.value_type == VALUE_TYPES[:integer]) ? "#{name}_id".to_sym  :
      name

    @filterable = options[:filterable]
    @group_by = options[:group_by]
    @aggregate_after_grouping = options[:aggregate_after_grouping]
    @primary = options[:primary] || false
    @dependency_respects_ordering = options[:dependency_respects_ordering]
    @null = options[:null]

    @generate_value = block

    if (denorm = options[:denormalized])
      raise InvalidSchemaDefinition, ":denormalized only makes sense for dimensions referring to a dimension type" unless @type.is_a?(Type::Dimension)
      @denormalized_columns = case denorm
                              when true then type.dimensions
                              when Array then denorm.map {|name| type[name]}
                              end
    end
  end

  # Columns of the referenced type for which a denormalized copy is included in the table for the parent fact or dimension type.
  #
  # Only makes sense for :on_table dimensions referring to a dimension type.
  #
  # This is the main tool which allows the schema to be defined logically in 3rd normal form, while still giving control
  # allowing the SQL schema to be denormalized for performance.
  #
  # todo: allow to specify a different column name for the denormalized column to the original
  attr_reader :denormalized_columns

  # SQL column name to be used for this dimension
  # if column name isn't specified, we presume same as the dimension name for dimensions with value types (eg 'title')
  # or same with _id appended for dimensions with dimension types referring to integer primary dimensions, which would be
  # a foreign key reference (eg 'release_id' for 'release')
  attr_reader :column_name

  # Is this dimension stored on the table of the parent fact or dimension type?
  # Useful when the parent dimension type does have a table, but the table doesn't include a column for all of its dimension,
  # with some of them only appearing when the dimension type is referenced in a denormalized setting.
  # Allows for finer-grained option that's less drastic than :table => false.
  # Eg common to allow lookup of a dimension's title via a separate table, but store the primary keys of dependent dimensions
  # only in a denormalized way
  def on_table?; @on_table; end

  # Can filters be applied to this dimension?
  # requires that its type supports filtering
  def filterable?; @filterable; end

  # Can this dimension be grouped by? (default true; you may want to disable this for dimensions whose values are too 'fine-grained' to be
  # usefully grouped by directly, eg datetime values; instead you probably want to group by a dependent dimension like the
  # days_since_epoch of that datetime)
  def group_by?; @group_by; end

  # Primary dimension - exactly one must be specified for a dimension type. Referenced by foreign keys
  def primary?; @primary; end

  # Should we support a second aggregation of the counts resulting from grouping by this dimension?
  # (default true; may want to disable this if there's only a small number of values, eg 'average listens per gender' isn't much use)
  def aggregate_after_grouping?; @aggregate_after_grouping; end

  # OK so there is a functional dependency of this dimension's values on those of the primary dimension of its type.
  # Sometimes (eg a 'year' column of a 'date' type) this function will respect the order structure, so
  # date1 <= date2 implies date1.year <= date2.year. (in mathmo speak: a (non-strict) monotonic function).
  # When this is the case, we can infer range constraints on values of the dependent dimension from a range
  # constraint on the parent. This helps in particular when constructing tables to left-join onto containing
  # all possible values of certain date/time types in certain ranges.
  def dependency_respects_ordering?; @dependency_respects_ordering; end

  # dimension value may be null?
  def null?; @null; end

  def [](name)
    @type[name]
  end

  # the value type for identifying values of this dimension.
  # same as the type for value types; the type of the primary dimension for dimension types (think foreign key)
  def value_type
    @type.value_type
  end

  def generate_value(primary_dimension_value)
    raise Error, "Couldn't generate dimension value from primary dimension value" unless @generate_value
    @generate_value.call(primary_dimension_value)
  end
end
