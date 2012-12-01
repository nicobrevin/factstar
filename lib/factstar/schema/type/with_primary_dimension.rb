module Facstar
  class Schema::Type::WithPrimaryDimension < Schema::Type

    attr_reader :primary_dimension

    def primary_dimensions
      [@primary_dimension]
    end

    # the value type which identifies values of this type (ie the type of the appropriate foreign key)
    def value_type
      @primary_dimension.type
    end


    # In the absence of an actual table, you may supply a generate_primary_dimension_values method. This should return an enumerable
    # yielding values for your primary dimension(s). It will be used to populate a temporary table to left join onto in some cases.
    #
    # Types which are infinite? must be / may require that they be passed a range in order to limit the range of values generated.
    # Other types may use a range if supplied to avoid work, although shouldn't expect one.
    #
    # Range start/end will be given in the internal representation of the value type of your primary dimension; generated values
    # should also be in the internal representation of the value_types of your dimensions.
    #
    # See also generate_dependent_dimension_values on Type::Dimension which will be used to fill out any dependent values for
    # each generated primary key in the case of Dimension types.
    def generate_primary_dimension_values(range=nil)
      raise NotImplementedError, "Can't generate possible primary dimension values for type #{name}"
    end

    def add_sequel_columns_for_dimension_paths(table_generator, paths=nil)
      paths ||= @dimensions_by_name.keys
      paths.map do |path|
        type = type_by_path(*path)
        col_name = [name, *path].join('.')
        null = Query::Column::DimensionPath.new(self, path).null?
        type.value_type.add_sequel_column(table_generator, col_name, :null => null)
      end
    end

    def create_table_for_dimension_paths(temp_table_context, table_name=self.name, paths=[], options={})
      this = self # instance_eval workaround
      temp_table_context.create_temp_table(options) do |table_generator|
        this.add_sequel_columns_for_dimension_paths(table_generator, paths)
      end
    end

    def create_and_populate_table_for_dimension_paths(temp_table_context, paths=[], range=nil, options={})
      raise Error, "Require a range filter to generate possible values for infinite type" if infinite? && !range

      paths = [[]] | paths
      table_name = create_table_for_dimension_paths(temp_table_context, table_name, paths, options={})

      rows = generate_primary_dimension_values(range).map do |primary_dimension_value|
        row = {}
        paths.each do |path|
          type = self; value = primary_dimension_value
          path.each do |name|
            dimension = type[name]
            value = dimension.generate_value(value)
            type = dimension.type
          end
          col_name = [self.name, *path].join('.')
          row[col_name] = type.value_type.internal_to_database(value)
        end
        row
      end

      temp_table_context.db[table_name].multi_insert(rows)
      table_name
    end

    # adds the ability to generate and populate a temp table with columns for the requested dimension paths, in the absence
    # of an sql table for the type
    def columns_and_table_with_joins_for_dimension_paths(paths, path_to_self=[self.name], paths_forced_not_null=[], range=nil, temp_table_context=nil)
      return super if table

      table_alias = path_to_self.join('.').to_sym
      table_name = create_and_populate_table_for_dimension_paths(temp_table_context, paths, range)

      columns = {}
      paths.each do |p|
        col_name = [name, *p].join('.').to_sym
        columns[p] = col_name.qualify(table_alias)
      end
      [columns, {
        :table          => table_name,
        :table_alias    => table_alias,
        :primary_key_column_name => name,
        :joins          => {}
      }]
    end
  end
end
