# frozen_string_literal: true
# encoding: utf-8

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
        # @since 2.0.0
        def initialize(view, pipeline, options = {})
          @view = view
          @pipeline = pipeline.dup
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

        def aggregate_spec(session)
          Builder::Aggregation.new(pipeline, view, options.merge(session: session)).specification
        end

        def new(options)
          Aggregation.new(view, pipeline, options)
        end

        def initial_query_op(session)
          Operation::Aggregate.new(aggregate_spec(session))
        end

        def valid_server?(server)
          if secondary_ok?
            true
          else
            description = server.description
            description.standalone? || description.mongos? || description.primary? || description.load_balancer?
          end
        end

        def secondary_ok?
          !write?
        end

        def send_initial_query(server, session)
          unless valid_server?(server)
            log_warn("Rerouting the Aggregation operation to the primary server - #{server.summary} is not suitable")
            server = cluster.next_primary(nil, session)
          end
          initial_query_op(session).execute(server, context: Operation::Context.new(client: client, session: session))
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
