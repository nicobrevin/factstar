# -*- coding: utf-8 -*-
require 'factstar/schema/type/with_primary_dimension'

module Factstar

  VALUE_TYPES = {}

  # A value type is a primitive type, a domain or set or single-column relation in relational terminology,
  # analagous to a column type in sql. Things like string, integer, etc
  #
  # In general there are 3 representations for a value of a value type:
  #   'external' representation for client input/output: something JSONable (eg string, int, float)
  #   'internal' representation for manipulation in ruby code: eg Date objects etc
  #   'database' representation: whatever value goes in or out for the relevant sequel database column type
  #
  # It knows how to translate its values between database representation and an external representation suitable for
  # input/output via json.
  #
  # A number of value types are declared and made available to all schemas, but schemas may register additional ones as
  # required.
  class Schema::Type::Value < Type::WithPrimaryDimension
    # Registers a new value type instance in the list of value type instances to be supported in all schemas.
    # You can pass a class definition body as your block; a new subclass of Type::Value will then be created
    # with it for this instance
    def self.register_new(name, *p, &block)
      klass = block ? const_set(name.to_s.camelize, Class.new(self, &block)) : self
      VALUE_TYPES[name] = klass.new(name, *p)
    end

    def check_value(types, value)
      unless [*types].any? {|type| value.is_a?(type)}
        raise InvalidQueryParams.new("Expected #{types.inspect}, got #{value.class}:#{value}")
      end
      value
    end

    def initialize(name, sequel_type, sequel_type_options={})
      super(nil, name)
      @sequel_type = sequel_type
      @sequel_type_options = sequel_type_options
      dimension(name, self)
    end

    # options for sequel's schema generator functions to generate a column suitable for storing values of this value type
    attr_reader :sequel_type, :sequel_type_options

    def add_sequel_column(table_generator, column_name=nil, options={})
      table_generator.column(column_name || name, sequel_type, sequel_type_options.merge!(options))
    end

    def primary_dimensions
      @dimensions
    end

    def database_to_external(value)
      internal_to_external(database_to_internal(value))
    end

    def external_to_database(value)
      internal_to_database(external_to_internal(value))
    end

    def external_to_internal(value)
      value
    end

    def internal_to_external(value)
      value
    end

    def internal_to_database(value)
      value
    end

    def database_to_internal(value)
      value
    end

    # the value type which identifies this type.
    def value_type
      self
    end

    # Value types are typically infinite, or near enough. Override to false if you're happy to generate a temporary table of all possible values.
    def infinite?; true; end

    # Is this a numeric type (meaningful to sum, avg, etc)
    def numeric?; false; end
  end

  # Declare the default available value types

  # todo: allow different length options to be declared for string value types in our schema
  Type::Value.register_new(:string, :varchar, :size => 255)

  Type::Value.register_new(:integer, :integer) do
    def external_to_internal(value)
      check_value(Integer, value)
    end

    def numeric?; true; end

    def generate_primary_dimension_values(range); range; end

    # Sometimes sequel gives us a BigDecimal for normal integers. bit annoying.
    def database_to_internal(value); value.to_i; end
  end

  Type::Value.register_new(:float, :float) do
    def external_to_internal(value)
      check_value(Float, value)
    end

    def numeric?; true; end

    # Sometimes sequel gives us a BigDecimal for normal floats. bit annoying.
    def database_to_internal(value); value.to_f; end
  end

  Type::Value.register_new(:decimal, :decimal) do

    def re_decimal
      /^(?:[0-9]*(?:\.[0-9]*)?)$|^(?:0)$/
    end

    def external_to_internal(value)
      # BigDecimal.new is very forgiving (it sets invalid stuff to zero), so
      # stick a regex check in to make sure we aren't given anything too retarded
      raise InvalidQueryParams, "Expected decimal string matching '#{re_decimal.to_s}', given: #{value}" unless re_decimal =~ value
      BigDecimal.new(value.to_s)
    end

    def numeric?; true; end
  end

  # An enum field - with internal values and corresponding friendly output names
  class Type::Value::Enum < Type::Value
    def initialize(name, value_mapping)
      @external_to_internal = value_mapping
      @internal_to_external = value_mapping.invert
      size = @internal_to_external.keys.map {|k| k.length}.max
      super(name, :char, :size => size)
    end

    def internal_to_external(value); @internal_to_external[value]; end

    def external_to_internal(value)
      @external_to_internal[value] or raise InvalidQueryParams, "Expected one of #{@external_to_internal.keys.join(', ')}, got #{value}"
    end

    def generate_primary_dimension_values(range=nil); @internal_to_external.keys; end

    def infinite?; false; end
  end

  Type::Value.register_new(:date, :date) do
    def internal_to_external(value); value.to_s; end

    def external_to_internal(value)
      begin
        Date.parse(value)
      rescue ArgumentError
        raise InvalidQueryParams, "invalid date string: #{value}"
      end
    end

    def generate_primary_dimension_values(range); range; end
  end

  # the time type is like datetime, but without timezone information.
  # times are converted to localtime before they are presented to the
  # database
  Type::Value.register_new(:time, :time) do
    def internal_to_external(value); value.iso8601; end
    def external_to_internal(value); ::Time.iso8601(value); end
    def internal_to_database(value); value.localtime; end
  end

  Type::Value.register_new(:datetime, :datetime) do
    def internal_to_external(value); value.iso8601; end
    def external_to_internal(value); ::Time.iso8601(value); end
  end

  Type::Value.register_new(:boolean, :boolean) do
    def external_to_internal(value)
      check_value([TrueClass, FalseClass], value)
    end
  end

  # Value types which use the Factstar::TimeField subclass value wrappers
  class Type::Value::TimeField < Type::Value
    def initialize(name, time_class, sequel_type)
      super(name, sequel_type)
      @time_class = time_class
      @all = (time_class.all if time_class.respond_to?(:all))
    end

    def database_to_internal(value)
      @time_class.new(value)
    end

    def internal_to_database(value)
      value.to_i
    end

    def internal_to_external(value)
      value.to_s
    end

    def external_to_internal(value)
      @time_class.from_time(::Time.iso8601(value))
    end

    def generate_primary_dimension_values(range)
      @all || range # nice! because TimeField defines succ and <=>
    end

    def infinite?; !@all; end
  end

  Type::Value::TimeField.register_new(:half_hours_since_epoch,  TimeField::HalfHour,      :mediumint)
  Type::Value::TimeField.register_new(:days_since_epoch,        TimeField::Day,           :smallint)
  Type::Value::TimeField.register_new(:year_since_epoch,        TimeField::Year,          :smallint)
  Type::Value::TimeField.register_new(:weeks_since_epoch,       TimeField::Week,          :smallint)
  Type::Value::TimeField.register_new(:day_of_week,             TimeField::DayOfWeek,     :smallint)
  Type::Value::TimeField.register_new(:half_hour_of_day,        TimeField::HalfHourOfDay, :smallint)
  Type::Value::TimeField.register_new(:month_of_year,           TimeField::MonthOfYear,   :smallint)
end
