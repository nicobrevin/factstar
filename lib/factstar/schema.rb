# A 'snowflake' style reporting schema in 3rd normal form (no transitive
# dependencies).
#
# A schema is a set of types registered by name. Types may be value types,
# dimension types or fact types;
# see the docs for these classes for some more detail
class Factstar::Schema

  # Sequel::Database
  attr_reader :database

  def initialize(database, &block)
    @database = database
    @types_by_name = {}
    # All schemas have the standard set of value types available by default
    VALUE_TYPES.each_value {|t| register_type(t)}
    instance_eval(&block) if block_given?
  end

  def fact_type(name, options={}, &block)
    register_type Type::Fact.new(self, name, options, &block)
  end

  def dimension_type(name, options={}, &block)
    register_type Type::Dimension.new(self, name, options, &block)
  end

  def value_type(name, klass, *p)
    register_type klass.new(name, *p)
  end

  def register_type(type)
    raise InvalidSchemaDefinition, "Type #{type.name} already registered" if @types_by_name[type.name]
    @types_by_name[type.name.to_s] = type
  end

  def [](name)
    @types_by_name[name.to_s]
  end

  def each(&block)
    @types_by_name.each(&block)
  end

  def dimensions
    @types_by_name.map {|n, v| v.is_a?(Type::Dimension) ? v : nil }.compact
  end

  def new_query(options)
    Query.new(self, options)
  end
end

