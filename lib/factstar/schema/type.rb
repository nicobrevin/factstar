# Abstract superclass.
# A type is a relation in relational terminology; we restrict the kinds of
# relations we allow though to a snowflake-style reporting schema
class Type
  attr_reader :schema, :name, :dimensions_by_name, :primary_dimensions, :media_type

  def initialize(schema, name, options={}, &block)
    @schema = schema
    @name = name
    @dimensions_by_name = {}
    @table = options[:table] || nil
    @table_filters = options[:table_filters]

    # FIXME: bit MSP-specific this, but a media type for types corresponding to MSP::Resource::Base media types identified by their id:
    @media_type = options[:class] && options[:class].media_type_prefix

    instance_eval(&block) if block
  end

  def dimensions; @dimensions_by_name.values; end

  def dimension(name, type, options={}, &block)
    raise InvalidSchemaDefinition, "Dimension #{name} already registered" if @dimensions_by_name[name.to_s]
    type = @schema[type] || raise(InvalidSchemaDefinition, "Type #{type} not found in schema yet") if type.is_a?(Symbol)
    @dimensions_by_name[name.to_s] = MSP::Reports2::Dimension.new(name, type, options, &block)
  end

  def [](name)
    @dimensions_by_name[name.to_s] or raise "Dimension #{name} not found on type #{@name}"
  end

  def type_by_path(*names)
    names.inject(self) {|type,name| type[name].type}
  end

  def dimension_by_path(name, *more_names)
    more_names.inject(self[name]) {|dim,name| dim[name]}
  end

  def inspect
    "#<#{self.class}: #{@name}>"
  end

  # the SQL table on which all possible values of this type's dimensions (together with any denormalized dimensions from
  # referenced dimension types) are stored. May be nil if the type doesn't have its own separate table (which may be the
  # case when it's just there to tell the schema about a functional dependency, but that dependency is always denormalized
  # in practise; or where it's a value type and the possible values may be infinite, say)
  attr_reader :table

  # Allows a filtered subset of a table, rather than the whole table, to be used for storing the dimensions of this type.
  # like using an updateable view rather than a physical table. Useful if you want to store some columns for multiple dimensions
  # in one physical table.
  #
  # Why specify table and table_filters separately, rather than just allow a dataset? because this allows us to build left
  # joins with the table filters built into the left join condition, which is preferable to using subqueries in some cases.
  # Also it allows us to see what fixed values we need to populate new rows with when populating the table. Bit like an
  # updateable view, except we have to do the work ourself.
  attr_reader :table_filters

  # Does this type have an infinite set of values?
  def infinite?; false; end


  # This contains the central logic that decides how to construct a bunch of joins to select a particular 'dimension path',
  # that is an array of subsequent dimension names like [:track, :release, :label, :title] for the title of a label of a release of a track.
  # It takes into account denormalized_columns, table names, column names. The inner steps in converting the nice 3NF model into something SQLey.
  #
  # guaranteed_not_null - the biggest prefix of the given path for which we're guaranteed (by filter conditions) has only non-null values (despite
  # the dimension in the underlying schema potentially being nullable)
  def column_and_joins_for_dimension_path(path, path_to_self=[self.name], guaranteed_not_null=nil, parent_table_alias=nil, came_via_dimension=nil, left_join=false)
    path = [primary_dimension.name] if path.empty? && is_a?(Type::WithPrimaryDimension)
    next_name, *remaining_names = *path

    dimension = @dimensions_by_name[next_name.to_s] or raise "Couldn't resolve dimension path #{path.join('.')}"
    path_to_dimension = path_to_self + [next_name]

    if came_via_dimension && (denorm = came_via_dimension.denormalized_columns) && denorm.include?(dimension)
      table_alias = parent_table_alias
      joins = more_joins = {}
    else
      table_alias = path_to_self.join('.').to_sym
      if came_via_dimension
        # only WithPrimaryDimension types will come via a dimension of another type
        joins = {came_via_dimension.column_name => {
            :table_alias    => table_alias,
            :table          => table,
            :table_filters  => table_filters,
            :left_join      => left_join,
            :primary_key_column_name => primary_dimension.column_name,
            :joins          => (more_joins = {})
          }}
      else
        joins = more_joins = {}
      end
    end

    if remaining_names.empty?
      column = dimension.column_name.qualify(table_alias)
    else
      further_left_join = left_join || (dimension.null? && !guaranteed_not_null)
      further_guaranteed_not_null = if guaranteed_not_null && guaranteed_not_null.length > 1 then guaranteed_not_null[1..-1] end

      column, further_joins = dimension.type.column_and_joins_for_dimension_path(remaining_names, path_to_dimension, further_guaranteed_not_null, table_alias, dimension, further_left_join)
      more_joins.replace(further_joins)
    end

    [column, joins]
  end

  # Returns a column mapping and a table with joins spec for the following paths from the current type.
  # Some implementations may create temporary tables populated with values in order to do this, in which case you may
  # need to pass a temp_table_context and (in the case of an infinite? type) a range in order for this to work.
  # It's also assumed you'll clean up the temp table afterwards - you can tell because it begins with __temp
  # (todo: cleaner way to tell).
  # Superclass implementation knows how to do this based off an sql table where present.
  def columns_and_table_with_joins_for_dimension_paths(paths, path_to_self=[self.name], paths_forced_not_null=[], range=nil, temp_table_context=nil)
    raise NotImplementedException unless table

    columns = {}; joins = {}
    paths.each do |p|
      # try and find the biggest (if any) prefix of this path which is forced_not_null;
      # this will help column_and_joins_for_dimension_path to avoid any left joins up to that point.
      prefixes = (p.length-1).downto(0).map {|i| p[0..i]}
      guaranteed_not_null = prefixes.find {|prefix| paths_forced_not_null.include?(prefix)}

      column, more_joins = column_and_joins_for_dimension_path(p, path_to_self, guaranteed_not_null)
      columns[p] = column
      Utils.merge_joins!(joins, more_joins)
    end
    [columns, {
        :table          => table,
        :table_filters  => table_filters,
        :table_alias    => path_to_self.join('.').to_sym,
        :joins          => joins
      }]
  end
end
