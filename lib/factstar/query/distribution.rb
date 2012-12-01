module Factstar

  # A distribution query can take any query which has an integer or float column
  class Query::Distribution < Query

    attr_reader :query, :num_buckets, :dataset

    def initialize(query, num_buckets=10)
      @query = query
      @num_buckets = num_buckets
    end

    def db
      @query.db
    end

    def execute(&block)
      TempTableContext.with_context(db) do |context|
        setup(context)
        super(&block)
      end
    end

    def setup(temp_table_context)
      @query.execute_in_context(temp_table_context) do |rows|
        @column = @query.columns.first
        unless @query.columns.length == 1 && @column.value_type.numeric?
          raise InvalidQueryParams, "distribution query only supported for queries resulting in a single integer or float column"
        end

        distribution_table = temp_table_context.create_temp_table_from_dataset(@query.dataset)
        column_name = @column.column_alias
        qualified_column_name = column_name.qualify(distribution_table)

        stats = db[distribution_table].select(:max.sql_function(column_name).as(:max),
          :min.sql_function(column_name).as(:min), :count.sql_function(column_name).as(:count)).first

        buckets_table = temp_table_context.create_temp_table do
          integer :start, :null => false
          integer :finish, :null => false
        end

        # We adjust the number of buckets used in practise in order to align buckets to integer boundaries. This may result
        # in less buckets than were requested.
        # (TODO: pick finer or coarser-grained discrete boundaries to align buckets to depending on size of the spread?).
        if stats[:min]
          bucket_start = stats[:min].floor
          bucket_spread_int = (stats[:max] + 1).floor - bucket_start
          bucket_size = (bucket_spread_int.to_f / @num_buckets).ceil
          use_num_buckets = (bucket_spread_int.to_f / bucket_size).ceil

          buckets = (0...use_num_buckets).map do |bucket|
            {:start => (start = bucket_start + bucket*bucket_size), :finish => start + bucket_size}
          end
          db[buckets_table].multi_insert(buckets)
        end

        # Buckets are all of equal width, so we don't give a bucket-size-normalised frequency figure, just a count per bucket.

        @dataset = db[distribution_table].
          select(:count.sql_function(qualified_column_name).as(:count), :start, :finish).
          right_outer_join(buckets_table, (qualified_column_name >= :start) & (qualified_column_name < :finish)).
          group(:start, :finish)
      end
    end

    def columns
      [
        Query::Column.new(:start,  :float),
        Query::Column.new(:finish, :float),
        Query::Column.new(:count,  :integer)
      ]
    end
  end
end
