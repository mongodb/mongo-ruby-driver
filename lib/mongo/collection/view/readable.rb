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

        # Special fields and their getters for the query selector.
        #
        # @since 2.0.0
        SPECIAL_FIELDS = {
          :$query => :selector,
          :$readPreference => :read_pref_formatted,
          :$orderby => :sort,
          :$hint => :hint,
          :$comment => :comment,
          :$snapshot => :snapshot,
          :$maxScan => :max_scan,
          :$showDiskLoc => :show_disk_loc,
          :$explain => :explained?
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
          configure(:comment, comment)
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
          database.command(cmd, options).n
        end

        # Get a list of distinct values for a specific field.
        #
        # @example Get the distinct values.
        #   collection_view.distinct('name')
        #
        # @param [ String, Symbol ] field_name The name of the field.
        # @param [ Hash ] options Options for the distinct command.
        #
        # @option options :read [ Hash ] The read preference for this command.
        #
        # @return [ Array<Object> ] The list of distinct values.
        #
        # @since 2.0.0
        def distinct(field_name, options={})
          database.command({
            :distinct => collection.name,
            :key => field_name.to_s,
            :query => selector },
            options
          ).documents.first['values']
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
          configure(:read, value)
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

        private

        def default_read(read = nil)
          options[:read] || read_preference
        end

        def flags
          @flags ||= (!primary? ? [ :slave_ok ] : [])
        end

        def has_special_fields?
          sort || hint || comment || max_scan || show_disk_loc || snapshot || explained? || cluster.sharded?
        end

        def primary?
          read.name == :primary
        end

        def query_options
          { :project => projection, :skip => skip, :limit => to_return, :flags => flags }
        end

        def query_spec
          sel = has_special_fields? ? special_selector : selector
          { :selector  => sel,
            :read      => read,
            :options   => query_options,
            :db_name   => database.name,
            :coll_name => collection.name }
        end

        def read_pref_formatted
          read.to_mongos
        end

        def special_selector
          SPECIAL_FIELDS.reduce({}) do |hash, (key, method)|
            value = send(method)
            hash[key] = value if value
            hash
          end
        end

        def to_return
          [ limit || batch_size, batch_size || limit ].min
        end

        def validate_doc!(doc)
          raise Error::InvalidDocument.new unless doc.respond_to?(:keys)
        end
      end
    end
  end
end
