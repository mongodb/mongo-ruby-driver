# Copyright (C) 2014-2017 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  class Collection
    class View

      # Defines read related behaviour for collection view.
      #
      # @since 2.0.0
      module Readable

        # The query modifier constant.
        #
        # @since 2.2.0
        QUERY = '$query'.freeze

        # The modifiers option constant.
        #
        # @since 2.2.0
        MODIFIERS = 'modifiers'.freeze

        # Execute an aggregation on the collection view.
        #
        # @example Aggregate documents.
        #   view.aggregate([
        #     { "$group" => { "_id" => "$city", "tpop" => { "$sum" => "$pop" }}}
        #   ])
        #
        # @param [ Array<Hash> ] pipeline The aggregation pipeline.
        # @param [ Hash ] options The aggregation options.
        #
        # @return [ Aggregation ] The aggregation object.
        #
        # @since 2.0.0
        def aggregate(pipeline, options = {})
          Aggregation.new(self, pipeline, options)
        end

        # Allows the query to get partial results if some shards are down.
        #
        # @example Allow partial results.
        #   view.allow_partial_results
        #
        # @return [ View ] The new view.
        #
        # @since 2.0.0
        def allow_partial_results
          configure(:allow_partial_results, true)
        end

        # Tell the query's cursor to stay open and wait for data.
        #
        # @example Await data on the cursor.
        #   view.await_data
        #
        # @return [ View ] The new view.
        #
        # @since 2.0.0
        def await_data
          configure(:await_data, true)
        end

        # The number of documents returned in each batch of results from MongoDB.
        #
        # @example Set the batch size.
        #   view.batch_size(5)
        #
        # @note Specifying 1 or a negative number is analogous to setting a limit.
        #
        # @param [ Integer ] batch_size The size of each batch of results.
        #
        # @return [ Integer, View ] Either the batch_size value or a
        # new +View+.
        #
        # @since 2.0.0
        def batch_size(batch_size = nil)
          configure(:batch_size, batch_size)
        end

        # Associate a comment with the query.
        #
        # @example Add a comment.
        #   view.comment('slow query')
        #
        # @note Set profilingLevel to 2 and the comment will be logged in the profile
        #   collection along with the query.
        #
        # @param [ String ] comment The comment to be associated with the query.
        #
        # @return [ String, View ] Either the comment or a
        #   new +View+.
        #
        # @since 2.0.0
        def comment(comment = nil)
          configure(:comment, comment)
        end

        # Get a count of matching documents in the collection.
        #
        # @example Get the number of documents in the collection.
        #   collection_view.count
        #
        # @param [ Hash ] opts Options for the count command.
        #
        # @option opts :skip [ Integer ] The number of documents to skip.
        # @option opts :hint [ Hash ] Override default index selection and force
        #   MongoDB to use a specific index for the query.
        # @option opts :limit [ Integer ] Max number of docs to return.
        # @option opts :max_time_ms [ Integer ] The maximum amount of time to allow the
        #   command to run.
        # @option opts [ Hash ] :read The read preference options.
        # @option opts [ Hash ] :collation The collation to use.
        #
        # @return [ Integer ] The document count.
        #
        # @since 2.0.0
        def count(opts = {})
          cmd = { :count => collection.name, :query => filter }
          cmd[:skip] = opts[:skip] if opts[:skip]
          cmd[:hint] = opts[:hint] if opts[:hint]
          cmd[:limit] = opts[:limit] if opts[:limit]
          cmd[:maxTimeMS] = opts[:max_time_ms] if opts[:max_time_ms]
          cmd[:readConcern] = collection.read_concern if collection.read_concern
          preference = ServerSelector.get(opts[:read] || read)
          read_with_retry do
            server = preference.select_server(cluster, false)
            apply_collation!(cmd, server, opts)
            Operation::Commands::Command.new({
                                               :selector => cmd,
                                               :db_name => database.name,
                                               :options => { :limit => -1 },
                                               :read => preference,
                                             }).execute(server).n.to_i

          end
        end

        # Get a list of distinct values for a specific field.
        #
        # @example Get the distinct values.
        #   collection_view.distinct('name')
        #
        # @param [ String, Symbol ] field_name The name of the field.
        # @param [ Hash ] opts Options for the distinct command.
        #
        # @option opts :max_time_ms [ Integer ] The maximum amount of time to allow the
        #   command to run.
        # @option opts [ Hash ] :read The read preference options.
        # @option opts [ Hash ] :collation The collation to use.
        #
        # @return [ Array<Object> ] The list of distinct values.
        #
        # @since 2.0.0
        def distinct(field_name, opts = {})
          cmd = { :distinct => collection.name,
                  :key => field_name.to_s,
                  :query => filter }
          cmd[:maxTimeMS] = opts[:max_time_ms] if opts[:max_time_ms]
          cmd[:readConcern] = collection.read_concern if collection.read_concern
          preference = ServerSelector.get(opts[:read] || read)
          read_with_retry do
            server = preference.select_server(cluster, false)
            apply_collation!(cmd, server, opts)
            Operation::Commands::Command.new({
                                               :selector => cmd,
                                               :db_name => database.name,
                                               :options => { :limit => -1 },
                                               :read => preference
                                             }).execute(server).first['values']

          end
        end

        # The index that MongoDB will be forced to use for the query.
        #
        # @example Set the index hint.
        #   view.hint(name: 1)
        #
        # @param [ Hash ] hint The index to use for the query.
        #
        # @return [ Hash, View ] Either the hint or a new +View+.
        #
        # @since 2.0.0
        def hint(hint = nil)
          configure(:hint, hint)
        end

        # The max number of docs to return from the query.
        #
        # @example Set the limit.
        #   view.limit(5)
        #
        # @param [ Integer ] limit The number of docs to return.
        #
        # @return [ Integer, View ] Either the limit or a new +View+.
        #
        # @since 2.0.0
        def limit(limit = nil)
          configure(:limit, limit)
        end

        # Execute a map/reduce operation on the collection view.
        #
        # @example Execute a map/reduce.
        #   view.map_reduce(map, reduce)
        #
        # @param [ String ] map The map js function.
        # @param [ String ] reduce The reduce js function.
        # @param [ Hash ] options The map/reduce options.
        #
        # @return [ MapReduce ] The map reduce wrapper.
        #
        # @since 2.0.0
        def map_reduce(map, reduce, options = {})
          MapReduce.new(self, map, reduce, options)
        end

        # Set the max number of documents to scan.
        #
        # @example Set the max scan value.
        #   view.max_scan(1000)
        #
        # @param [ Integer ] value The max number to scan.
        #
        # @return [ Integer, View ] The value or a new +View+.
        #
        # @since 2.0.0
        def max_scan(value = nil)
          configure(:max_scan, value)
        end

        # Set the maximum value to search.
        #
        # @example Set the max value.
        #   view.max_value(_id: 1)
        #
        # @param [ Hash ] value The max field and value.
        #
        # @return [ Hash, View ] The value or a new +View+.
        #
        # @since 2.1.0
        def max_value(value = nil)
          configure(:max_value, value)
        end

        # Set the minimum value to search.
        #
        # @example Set the min value.
        #   view.min_value(_id: 1)
        #
        # @param [ Hash ] value The min field and value.
        #
        # @return [ Hash, View ] The value or a new +View+.
        #
        # @since 2.1.0
        def min_value(value = nil)
          configure(:min_value, value)
        end

        # The server normally times out idle cursors after an inactivity period
        # (10 minutes) to prevent excess memory use. Set this option to prevent that.
        #
        # @example Set the cursor to not timeout.
        #   view.no_cursor_timeout
        #
        # @return [ View ] The new view.
        #
        # @since 2.0.0
        def no_cursor_timeout
          configure(:no_cursor_timeout, true)
        end

        # The fields to include or exclude from each doc in the result set.
        #
        # @example Set the fields to include or exclude.
        #   view.projection(name: 1)
        #
        # @note A value of 0 excludes a field from the doc. A value of 1 includes it.
        #   Values must all be 0 or all be 1, with the exception of the _id value.
        #   The _id field is included by default. It must be excluded explicitly.
        #
        # @param [ Hash ] document The field and 1 or 0, to include or exclude it.
        #
        # @return [ Hash, View ] Either the fields or a new +View+.
        #
        # @since 2.0.0
        def projection(document = nil)
          validate_doc!(document) if document
          configure(:projection, document)
        end

        # The read preference to use for the query.
        #
        # @note If none is specified for the query, the read preference of the
        #   collection will be used.
        #
        # @param [ Hash ] value The read preference mode to use for the query.
        #
        # @return [ Symbol, View ] Either the read preference or a
        #   new +View+.
        #
        # @since 2.0.0
        def read(value = nil)
          return default_read if value.nil?
          selector = ServerSelector.get(value)
          configure(:read, selector)
        end

        # Set whether to return only the indexed field or fields.
        #
        # @example Set the return key value.
        #   view.return_key(true)
        #
        # @param [ true, false ] value The return key value.
        #
        # @return [ true, false, View ] The value or a new +View+.
        #
        # @since 2.1.0
        def return_key(value = nil)
          configure(:return_key, value)
        end

        # Set whether the disk location should be shown for each document.
        #
        # @example Set show disk location option.
        #   view.show_disk_loc(true)
        #
        # @param [ true, false ] value The value for the field.
        #
        # @return [ true, false, View ] Either the value or a new
        #   +View+.
        #
        # @since 2.0.0
        def show_disk_loc(value = nil)
          configure(:show_disk_loc, value)
        end
        alias :show_record_id :show_disk_loc

        # The number of docs to skip before returning results.
        #
        # @example Set the number to skip.
        #   view.skip(10)
        #
        # @param [ Integer ] number Number of docs to skip.
        #
        # @return [ Integer, View ] Either the skip value or a
        #   new +View+.
        #
        # @since 2.0.0
        def skip(number = nil)
          configure(:skip, number)
        end

        # Set the snapshot value for the view.
        #
        # @note When set to true, prevents documents from returning more than
        #   once.
        #
        # @example Set the snapshot value.
        #   view.snapshot(true)
        #
        # @param [ true, false ] value The snapshot value.
        #
        # @since 2.0.0
        def snapshot(value = nil)
          configure(:snapshot, value)
        end

        # The key and direction pairs by which the result set will be sorted.
        #
        # @example Set the sort criteria
        #   view.sort(name: -1)
        #
        # @param [ Hash ] spec The attributes and directions to sort by.
        #
        # @return [ Hash, View ] Either the sort setting or a
        #   new +View+.
        #
        # @since 2.0.0
        def sort(spec = nil)
          configure(:sort, spec)
        end

        # “meta” operators that let you modify the output or behavior of a query.
        #
        # @example Set the modifiers document.
        #   view.modifiers(:$orderby => Mongo::Index::ASCENDING)
        #
        # @param [ Hash ] doc The modifiers document.
        #
        # @return [ Hash, View ] Either the modifiers document or a new +View+.
        #
        # @since 2.1.0
        def modifiers(doc = nil)
          return Builder::Modifiers.map_server_modifiers(options) if doc.nil?
          new(options.merge(Builder::Modifiers.map_driver_options(doc)))
        end

        # A cumulative time limit in milliseconds for processing get more operations
        # on a cursor.
        #
        # @example Set the max await time ms value.
        #   view.max_await_time_ms(500)
        #
        # @param [ Integer ] max The max time in milliseconds.
        #
        # @return [ Integer, View ] Either the max await time ms value or a new +View+.
        #
        # @since 2.1.0
        def max_await_time_ms(max = nil)
          configure(:max_await_time_ms, max)
        end

        # A cumulative time limit in milliseconds for processing operations on a cursor.
        #
        # @example Set the max time ms value.
        #   view.max_time_ms(500)
        #
        # @param [ Integer ] max The max time in milliseconds.
        #
        # @return [ Integer, View ] Either the max time ms value or a new +View+.
        #
        # @since 2.1.0
        def max_time_ms(max = nil)
          configure(:max_time_ms, max)
        end

        # The type of cursor to use. Can be :tailable or :tailable_await.
        #
        # @example Set the cursor type.
        #   view.cursor_type(:tailable)
        #
        # @param [ :tailable, :tailable_await ] type The cursor type.
        #
        # @return [ :tailable, :tailable_await, View ] Either the cursor type setting or a new +View+.
        #
        # @since 2.3.0
        def cursor_type(type = nil)
          configure(:cursor_type, type)
        end

        private

        def collation(doc = nil)
          configure(:collation, doc)
        end

        def default_read
          options[:read] || collection.read_preference
        end

        def parallel_scan(cursor_count, options = {})
          server = read.select_server(cluster, false)
          cmd = Operation::Commands::ParallelScan.new({
                  :coll_name => collection.name,
                  :db_name => database.name,
                  :cursor_count => cursor_count,
                  :read_concern => collection.read_concern
                }.merge!(options))
          cmd.execute(server).cursor_ids.map do |cursor_id|
            result = if server.features.find_command_enabled?
                Operation::Commands::GetMore.new({
                  :selector => { :getMore => cursor_id, :collection => collection.name },
                  :db_name  => database.name
                }).execute(server)
              else
                Operation::Read::GetMore.new({
                  :to_return => 0,
                  :cursor_id => cursor_id,
                  :db_name   => database.name,
                  :coll_name => collection.name
                }).execute(server)
            end
            Cursor.new(self, result, server)
          end
        end

        def validate_doc!(doc)
          raise Error::InvalidDocument.new unless doc.respond_to?(:keys)
        end
      end
    end
  end
end
