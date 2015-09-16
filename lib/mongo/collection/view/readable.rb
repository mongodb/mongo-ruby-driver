# Copyright (C) 2014-2015 MongoDB, Inc.
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

        # Special fields and their option names for the query selector.
        #
        # @since 2.0.0
        SPECIAL_FIELDS = {
          :sort => :$orderby,
          :hint => :$hint,
          :comment => :$comment,
          :snapshot => :$snapshot,
          :max_scan => :$maxScan,
          :max_value => :$max,
          :min_value => :$min,
          :max_time_ms => :$maxTimeMS,
          :return_key => :$returnKey,
          :show_disk_loc => :$showDiskLoc,
          :explain => :$explain
        }.freeze

        # Options to cursor flags mapping.
        #
        # @since 2.1.0
        CURSOR_FLAGS_MAP = {
          :allow_partial_results => [ :partial ],
          :oplog_replay => [ :oplog_replay ],
          :no_cursor_timeout => [ :no_cursor_timeout ],
          :tailable => [ :tailable_cursor ],
          :tailable_await => [ :await_data, :tailable_cursor]
        }.freeze

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
          configure_flag(:partial)
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
          configure_modifier(:comment, comment)
        end

        # Get a count of matching documents in the collection.
        #
        # @example Get the number of documents in the collection.
        #   collection_view.count
        #
        # @param [ Hash ] options Options for the count command.
        #
        # @option options :skip [ Integer ] The number of documents to skip.
        # @option options :hint [ Hash ] Override default index selection and force
        #   MongoDB to use a specific index for the query.
        # @option options :limit [ Integer ] Max number of docs to return.
        # @option options :max_time_ms [ Integer ] The maximum amount of time to allow the
        #   command to run.
        # @option options :read [ Hash ] The read preference for this command.
        #
        # @return [ Integer ] The document count.
        #
        # @since 2.0.0
        def count(options = {})
          cmd = { :count => collection.name, :query => selector }
          cmd[:skip] = options[:skip] if options[:skip]
          cmd[:hint] = options[:hint] if options[:hint]
          cmd[:limit] = options[:limit] if options[:limit]
          cmd[:maxTimeMS] = options[:max_time_ms] if options[:max_time_ms]
          read_with_retry do
            database.command(cmd, options).n.to_i
          end
        end

        # Get a list of distinct values for a specific field.
        #
        # @example Get the distinct values.
        #   collection_view.distinct('name')
        #
        # @param [ String, Symbol ] field_name The name of the field.
        # @param [ Hash ] options Options for the distinct command.
        #
        # @option options :max_time_ms [ Integer ] The maximum amount of time to allow the
        #   command to run.
        # @option options :read [ Hash ] The read preference for this command.
        #
        # @return [ Array<Object> ] The list of distinct values.
        #
        # @since 2.0.0
        def distinct(field_name, options={})
          cmd = { :distinct => collection.name,
                  :key => field_name.to_s,
                  :query => selector }
          cmd[:maxTimeMS] = options[:max_time_ms] if options[:max_time_ms]
          read_with_retry do
            database.command(cmd, options).first['values']
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
          configure_modifier(:hint, hint)
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
          configure_modifier(:max_scan, value)
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
          configure_modifier(:max_value, value)
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
          configure_modifier(:min_value, value)
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
          configure_flag(:no_cursor_timeout)
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
          selector = value.is_a?(Hash) ? ServerSelector.get(client.options.merge(value)) : value
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
          configure_modifier(:return_key, value)
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
          configure_modifier(:show_disk_loc, value)
        end

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
          configure_modifier(:snapshot, value)
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
          configure_modifier(:sort, spec)
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
          return @modifiers if doc.nil?
          new(options.merge(:modifiers => doc))
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
          configure_modifier(:max_time_ms, max)
        end

        private

        def default_read
          options[:read] || read_preference
        end

        def flags
          @flags ||= CURSOR_FLAGS_MAP.each.reduce([]) do |flags, (key, value)|
            if options[key] || (options[:cursor_type] && options[:cursor_type] == key)
              flags.push(*value)
            end
            flags
          end
        end

        def parallel_scan(cursor_count)
          server = read.select_server(cluster)
          Operation::ParallelScan.new(
            :coll_name => collection.name,
            :db_name => database.name,
            :cursor_count => cursor_count
          ).execute(server.context).cursor_ids.map do |cursor_id|
            result = Operation::Read::GetMore.new({ :to_return => 0,
                                                    :cursor_id => cursor_id,
                                                    :db_name   => database.name,
                                                    :coll_name => collection.name
              }).execute(server.context)
            Cursor.new(self, result, server)
          end
        end

        def setup(sel, opts)
          setup_options(opts)
          setup_selector(sel)
        end

        def setup_options(opts)
          @options = opts ? opts.dup : {}
          @modifiers = @options[:modifiers] ? @options.delete(:modifiers).dup : BSON::Document.new
          @options.keys.each { |k| @modifiers.merge!(SPECIAL_FIELDS[k] => @options.delete(k)) if SPECIAL_FIELDS[k] }
          @options.freeze
        end

        def setup_selector(sel)
          @selector = sel ? sel.dup : {}
          if @selector[:$query] || @selector['$query']
            @selector.keys.each { |k| @modifiers.merge!(k => @selector.delete(k)) if k[0] == '$' }
          end
          @modifiers.freeze
          @selector.freeze
        end

        def query_options
          {
            :project => projection,
            :skip => skip,
            :limit => limit,
            :flags => flags,
            :batch_size => batch_size
          }
        end

        def requires_special_selector?
          !modifiers.empty? || cluster.sharded?
        end

        def query_spec
          sel = requires_special_selector? ? special_selector : selector
          { :selector  => sel,
            :read      => read,
            :options   => query_options,
            :db_name   => database.name,
            :coll_name => collection.name }
        end

        def read_pref_formatted
          @read_formatted ||= read.to_mongos
        end

        def special_selector
          sel = BSON::Document.new(:$query => selector).merge!(modifiers)
          sel[:$readPreference] = read_pref_formatted unless read_pref_formatted.nil?
          sel
        end

        def validate_doc!(doc)
          raise Error::InvalidDocument.new unless doc.respond_to?(:keys)
        end
      end
    end
  end
end
