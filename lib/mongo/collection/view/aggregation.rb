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

      # Provides behavior around an aggregation pipeline on a collection view.
      #
      # @since 2.0.0
      class Aggregation
        extend Forwardable
        include Enumerable
        include Immutable
        include Iterable
        include Explainable
        include Loggable
        include Retryable

        # @return [ View ] view The collection view.
        attr_reader :view
        # @return [ Array<Hash> ] pipeline The aggregation pipeline.
        attr_reader :pipeline

        # Delegate necessary operations to the view.
        def_delegators :view, :collection, :read, :cluster

        # Delegate necessary operations to the collection.
        def_delegators :collection, :database, :client

        # The reroute message.
        #
        # @since 2.1.0
        # @deprecated
        REROUTE = 'Rerouting the Aggregation operation to the primary server.'.freeze

        # Set to true if disk usage is allowed during the aggregation.
        #
        # @example Set disk usage flag.
        #   aggregation.allow_disk_use(true)
        #
        # @param [ true, false ] value The flag value.
        #
        # @return [ true, false, Aggregation ] The aggregation if a value was
        #   set or the value if used as a getter.
        #
        # @since 2.0.0
        def allow_disk_use(value = nil)
          configure(:allow_disk_use, value)
        end

        # Initialize the aggregation for the provided collection view, pipeline
        # and options.
        #
        # @example Create the new aggregation view.
        #   Aggregation.view.new(view, pipeline)
        #
        # @param [ Collection::View ] view The collection view.
        # @param [ Array<Hash> ] pipeline The pipeline of operations.
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
        # @since 2.0.0
        def initialize(view, pipeline, options = {})
          @view = view
          @pipeline = pipeline.dup
          unless Mongo.broken_view_aggregate || view.filter.empty?
            @pipeline.unshift(:$match => view.filter)
          end
          @options = BSON::Document.new(options).freeze
        end

        # Get the explain plan for the aggregation.
        #
        # @example Get the explain plan for the aggregation.
        #   aggregation.explain
        #
        # @return [ Hash ] The explain plan.
        #
        # @since 2.0.0
        def explain
          self.class.new(view, pipeline, options.merge(explain: true)).first
        end

        # Whether this aggregation will write its result to a database collection.
        #
        # @return [ Boolean ] Whether the aggregation will write its result
        #   to a collection.
        #
        # @api private
        def write?
          pipeline.any? { |op| op.key?('$out') || op.key?(:$out) || op.key?('$merge') || op.key?(:$merge) }
        end

        private

        def server_selector
          @view.send(:server_selector)
        end

        def aggregate_spec(session, read_preference)
          Builder::Aggregation.new(
            pipeline,
            view,
            options.merge(session: session, read_preference: read_preference)
          ).specification
        end

        def new(options)
          Aggregation.new(view, pipeline, options)
        end

        def initial_query_op(session, read_preference)
          Operation::Aggregate.new(aggregate_spec(session, read_preference))
        end

        # Return effective read preference for the operation.
        #
        # If the pipeline contains $merge or $out, and read preference specified
        # by user is secondary or secondary_preferred, and target server is below
        # 5.0, than this method returns primary read preference, because the
        # aggregation will be routed to primary. Otherwise return the original
        # read preference.
        #
        # See https://github.com/mongodb/specifications/blob/master/source/crud/crud.rst#read-preferences-and-server-selection
        #
        # @param [ Server::Connection ] connection The connection which
        #   will be used for the operation.
        # @return [ Hash | nil ] read preference hash that should be sent with
        #   this command.
        def effective_read_preference(connection)
          return unless view.read_preference
          return view.read_preference unless write?
          return view.read_preference unless [:secondary, :secondary_preferred].include?(view.read_preference[:mode])

          primary_read_preference = {mode: :primary}
          description = connection.description
          if description.primary?
            log_warn("Routing the Aggregation operation to the primary server")
            primary_read_preference
          elsif description.mongos? && !description.features.merge_out_on_secondary_enabled?
            log_warn("Routing the Aggregation operation to the primary server")
            primary_read_preference
          else
            view.read_preference
          end

        end

        def send_initial_query(server, session)
          server.with_connection do |connection|
            initial_query_op(
              session,
              effective_read_preference(connection)
            ).execute_with_connection(
              connection,
              context: Operation::Context.new(client: client, session: session)
            )
          end
        end

        # Skip, sort, limit, projection are specified as pipeline stages
        # rather than as options.
        def cache_options
          {
            namespace: collection.namespace,
            selector: pipeline,
            read_concern: view.read_concern,
            read_preference: view.read_preference,
            collation: options[:collation],
            # Aggregations can read documents from more than one collection,
            # so they will be cleared on every write operation.
            multi_collection: true,
          }
        end
      end
    end
  end
end
