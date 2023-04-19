# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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

      # Defines read related behavior for collection view.
      #
      # @since 2.0.0
      module Readable

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
        # @option options [ true, false ] :allow_disk_use Set to true if disk
        #   usage is allowed during the aggregation.
        # @option options [ Integer ] :batch_size The number of documents to return
        #   per batch.
        # @option options [ true, false ] :bypass_document_validation Whether or
        #   not to skip document level validation.
        # @option options [ Hash ] :collation The collation to use.
        # @option options [ Object ] :comment A user-provided
        #   comment to attach to this command.
        # @option options [ String ] :hint The index to use for the aggregation.
        # @option options [ Hash ] :let Mapping of variables to use in the pipeline.
        #   See the server documentation for details.
        # @option options [ Integer ] :max_time_ms The maximum amount of time in
        #   milliseconds to allow the aggregation to run.
        # @option options [ true, false ] :use_cursor Indicates whether the command
        #   will request that the server provide results using a cursor. Note that
        #   as of server version 3.6, aggregations always provide results using a
        #   cursor and this option is therefore not valid.
        # @option options [ Session ] :session The session to use.
        #
        # @return [ Aggregation ] The aggregation object.
        #
        # @since 2.0.0
        def aggregate(pipeline, options = {})
          options = @options.merge(options) unless Mongo.broken_view_options
          aggregation = Aggregation.new(self, pipeline, options)

          # Because the $merge and $out pipeline stages write documents to the
          # collection, it is necessary to clear the cache when they are performed.
          #
          # Opt to clear the entire cache rather than one namespace because
          # the $out and $merge stages do not have to write to the same namespace
          # on which the aggregation is performed.
          QueryCache.clear if aggregation.write?

          aggregation
        end

        # Allows the server to write temporary data to disk while executing
        # a find operation.
        #
        # @return [ View ] The new view.
        def allow_disk_use
          configure(:allow_disk_use, true)
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
        #   new +View+.
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
        # @param [ Object ] comment The comment to be associated with the query.
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
        # @param [ Hash ] opts Options for the operation.
        #
        # @option opts :skip [ Integer ] The number of documents to skip.
        # @option opts :hint [ Hash ] Override default index selection and force
        #   MongoDB to use a specific index for the query.
        # @option opts :limit [ Integer ] Max number of docs to count.
        # @option opts :max_time_ms [ Integer ] The maximum amount of time to allow the
        #   command to run.
        # @option opts [ Hash ] :read The read preference options.
        # @option opts [ Hash ] :collation The collation to use.
        # @option opts [ Mongo::Session ] :session The session to use for the operation.
        # @option opts [ Object ] :comment A user-provided
        #   comment to attach to this command.
        #
        # @return [ Integer ] The document count.
        #
        # @since 2.0.0
        #
        # @deprecated Use #count_documents or #estimated_document_count instead. However, note that
        #   the following operators will need to be substituted when switching to #count_documents:
        #     * $where should be replaced with $expr (only works on 3.6+)
        #     * $near should be replaced with $geoWithin with $center
        #     * $nearSphere should be replaced with $geoWithin with $centerSphere
        def count(opts = {})
          opts = @options.merge(opts) unless Mongo.broken_view_options
          cmd = { :count => collection.name, :query => filter }
          cmd[:skip] = opts[:skip] if opts[:skip]
          cmd[:hint] = opts[:hint] if opts[:hint]
          cmd[:limit] = opts[:limit] if opts[:limit]
          if read_concern
            cmd[:readConcern] = Options::Mapper.transform_values_to_strings(
              read_concern)
          end
          cmd[:maxTimeMS] = opts[:max_time_ms] if opts[:max_time_ms]
          Mongo::Lint.validate_underscore_read_preference(opts[:read])
          read_pref = opts[:read] || read_preference
          selector = ServerSelector.get(read_pref || server_selector)
          with_session(opts) do |session|
            read_with_retry(session, selector) do |server|
              Operation::Count.new(
                selector: cmd,
                db_name: database.name,
                options: {:limit => -1},
                read: read_pref,
                session: session,
                # For some reason collation was historically accepted as a
                # string key. Note that this isn't documented as valid usage.
                collation: opts[:collation] || opts['collation'] || collation,
                comment: opts[:comment],
              ).execute(server, context: Operation::Context.new(client: client, session: session))
            end.n.to_i
          end
        end

        # Get a count of matching documents in the collection.
        #
        # @example Get the number of documents in the collection.
        #   collection_view.count
        #
        # @param [ Hash ] opts Options for the operation.
        #
        # @option opts :skip [ Integer ] The number of documents to skip.
        # @option opts :hint [ Hash ] Override default index selection and force
        #   MongoDB to use a specific index for the query. Requires server version 3.6+.
        # @option opts :limit [ Integer ] Max number of docs to count.
        # @option opts :max_time_ms [ Integer ] The maximum amount of time to allow the
        #   command to run.
        # @option opts [ Hash ] :read The read preference options.
        # @option opts [ Hash ] :collation The collation to use.
        # @option opts [ Mongo::Session ] :session The session to use for the operation.
        # @option ops [ Object ] :comment A user-provided
        #   comment to attach to this command.
        #
        # @return [ Integer ] The document count.
        #
        # @since 2.6.0
        def count_documents(opts = {})
          opts = @options.merge(opts) unless Mongo.broken_view_options
          pipeline = [:'$match' => filter]
          pipeline << { :'$skip' => opts[:skip] } if opts[:skip]
          pipeline << { :'$limit' => opts[:limit] } if opts[:limit]
          pipeline << { :'$group' => { _id: 1, n: { :'$sum' => 1 } } }

          opts = opts.slice(:hint, :max_time_ms, :read, :collation, :session, :comment)
          opts[:collation] ||= collation

          first = aggregate(pipeline, opts).first
          return 0 unless first
          first['n'].to_i
        end

        # Gets an estimate of the count of documents in a collection using collection metadata.
        #
        # @example Get the number of documents in the collection.
        #   collection_view.estimated_document_count
        #
        # @param [ Hash ] opts Options for the operation.
        #
        # @option opts :max_time_ms [ Integer ] The maximum amount of time to allow the command to
        #   run.
        # @option opts [ Hash ] :read The read preference options.
        # @option opts [ Object ] :comment A user-provided
        #   comment to attach to this command.
        #
        # @return [ Integer ] The document count.
        #
        # @since 2.6.0
        def estimated_document_count(opts = {})
          unless view.filter.empty?
            raise ArgumentError, "Cannot call estimated_document_count when querying with a filter"
          end

          %i[limit skip].each do |opt|
            if options.key?(opt) || opts.key?(opt)
              raise ArgumentError, "Cannot call estimated_document_count when querying with #{opt}"
            end
          end

          opts = @options.merge(opts) unless Mongo.broken_view_options
          Mongo::Lint.validate_underscore_read_preference(opts[:read])
          read_pref = opts[:read] || read_preference
          selector = ServerSelector.get(read_pref || server_selector)
          with_session(opts) do |session|
            read_with_retry(session, selector) do |server|
              context = Operation::Context.new(client: client, session: session)
              cmd = { count: collection.name }
              cmd[:maxTimeMS] = opts[:max_time_ms] if opts[:max_time_ms]
              if read_concern
                cmd[:readConcern] = Options::Mapper.transform_values_to_strings(read_concern)
              end
              result = Operation::Count.new(
                selector: cmd,
                db_name: database.name,
                read: read_pref,
                session: session,
                comment: opts[:comment],
              ).execute(server, context: context)
              result.n.to_i
            end
          end
        rescue Error::OperationFailure => exc
          if exc.code == 26
            # NamespaceNotFound
            # This should only happen with the aggregation pipeline path
            # (server 4.9+). Previous servers should return 0 for nonexistent
            # collections.
            0
          else
            raise
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
        # @option options [ Object ] :comment A user-provided
        #   comment to attach to this command.
        #
        # @return [ Array<Object> ] The list of distinct values.
        #
        # @since 2.0.0
        def distinct(field_name, opts = {})
          if field_name.nil?
            raise ArgumentError, 'Field name for distinct operation must be not nil'
          end
          opts = @options.merge(opts) unless Mongo.broken_view_options
          cmd = { :distinct => collection.name,
                  :key => field_name.to_s,
                  :query => filter, }
          cmd[:maxTimeMS] = opts[:max_time_ms] if opts[:max_time_ms]
          if read_concern
            cmd[:readConcern] = Options::Mapper.transform_values_to_strings(
              read_concern)
          end
          Mongo::Lint.validate_underscore_read_preference(opts[:read])
          read_pref = opts[:read] || read_preference
          selector = ServerSelector.get(read_pref || server_selector)
          with_session(opts) do |session|
            read_with_retry(session, selector) do |server|
              Operation::Distinct.new(
                selector: cmd,
                db_name: database.name,
                options: {:limit => -1},
                read: read_pref,
                session: session,
                comment: opts[:comment],
                # For some reason collation was historically accepted as a
                # string key. Note that this isn't documented as valid usage.
                collation: opts[:collation] || opts['collation'] || collation,
              ).execute(server, context: Operation::Context.new(client: client, session: session))
            end.first['values']
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
          MapReduce.new(self, map, reduce, @options.merge(options))
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
        #
        # @deprecated This option is deprecated as of MongoDB server
        #   version 4.0.
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
          return read_preference if value.nil?
          configure(:read, value)
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
        #
        # @deprecated This option is deprecated as of MongoDB server
        #   version 4.0.
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

        # If called without arguments or with a nil argument, returns
        # the legacy (OP_QUERY) server modifiers for the current view.
        # If called with a non-nil argument, which must be a Hash or a
        # subclass, merges the provided modifiers into the current view.
        # Both string and symbol keys are allowed in the input hash.
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
          if doc.nil?
            Operation::Find::Builder::Modifiers.map_server_modifiers(options)
          else
            new(options.merge(Operation::Find::Builder::Modifiers.map_driver_options(BSON::Document.new(doc))))
          end
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

        # @api private
        def read_concern
          if options[:session] && options[:session].in_transaction?
            options[:session].send(:txn_read_concern) || collection.client.read_concern
          else
            collection.read_concern
          end
        end

        # @api private
        def read_preference
          @read_preference ||= begin
            # Operation read preference is always respected, and has the
            # highest priority. If we are in a transaction, we look at
            # transaction read preference and default to client, ignoring
            # collection read preference. If we are not in transaction we
            # look at collection read preference which defaults to client.
            rp = if options[:read]
              options[:read]
            elsif options[:session] && options[:session].in_transaction?
              options[:session].txn_read_preference || collection.client.read_preference
            else
              collection.read_preference
            end
            Lint.validate_underscore_read_preference(rp)
            rp
          end
        end

        private

        def collation(doc = nil)
          configure(:collation, doc)
        end

        def server_selector
          @server_selector ||= if options[:session] && options[:session].in_transaction?
            ServerSelector.get(read_preference || client.server_selector)
          else
            ServerSelector.get(read_preference || collection.server_selector)
          end
        end

        def parallel_scan(cursor_count, options = {})
          if options[:session]
            # The session would be overwritten by the one in +options+ later.
            session = client.send(:get_session, @options)
          else
            session = nil
          end
          server = server_selector.select_server(cluster, nil, session)
          spec = {
            coll_name: collection.name,
            db_name: database.name,
            cursor_count: cursor_count,
            read_concern: read_concern,
            session: session,
          }.update(options)
          session = spec[:session]
          op = Operation::ParallelScan.new(spec)
          # Note that the context object shouldn't be reused for subsequent
          # GetMore operations.
          context = Operation::Context.new(client: client, session: session)
          result = op.execute(server, context: context)
          result.cursor_ids.map do |cursor_id|
            spec = {
              cursor_id: cursor_id,
              coll_name: collection.name,
              db_name: database.name,
              session: session,
              batch_size: batch_size,
              to_return: 0,
              # max_time_ms is not being passed here, I assume intentionally?
            }
            op = Operation::GetMore.new(spec)
            context = Operation::Context.new(
              client: client,
              session: session,
              connection_global_id: result.connection_global_id,
            )
            result = op.execute(server, context: context)
            Cursor.new(self, result, server, session: session)
          end
        end

        def validate_doc!(doc)
          raise Error::InvalidDocument.new unless doc.respond_to?(:keys)
        end
      end
    end
  end
end
