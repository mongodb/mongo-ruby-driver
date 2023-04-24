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

      # Provides behavior around a map/reduce operation on the collection
      # view.
      #
      # @since 2.0.0
      class MapReduce
        extend Forwardable
        include Enumerable
        include Immutable
        include Loggable
        include Retryable

        # The inline option.
        #
        # @since 2.1.0
        INLINE = 'inline'.freeze

        # Reroute message.
        #
        # @since 2.1.0
        # @deprecated
        REROUTE = 'Rerouting the MapReduce operation to the primary server.'.freeze

        # @return [ View ] view The collection view.
        attr_reader :view

        # @return [ String ] map The map function.
        attr_reader :map_function

        # @return [ String ] reduce The reduce function.
        attr_reader :reduce_function

        # Delegate necessary operations to the view.
        def_delegators :view, :collection, :read, :cluster

        # Delegate necessary operations to the collection.
        def_delegators :collection, :database, :client

        # Iterate through documents returned by the map/reduce.
        #
        # @example Iterate through the result of the map/reduce.
        #   map_reduce.each do |document|
        #     p document
        #   end
        #
        # @return [ Enumerator ] The enumerator.
        #
        # @since 2.0.0
        #
        # @yieldparam [ Hash ] Each matching document.
        def each
          @cursor = nil
          session = client.send(:get_session, @options)
          server = cluster.next_primary(nil, session)
          result = send_initial_query(server, session, context: Operation::Context.new(client: client, session: session))
          result = send_fetch_query(server, session) unless inline?
          @cursor = Cursor.new(view, result, server, session: session)
          if block_given?
            @cursor.each do |doc|
              yield doc
            end
          else
            @cursor.to_enum
          end
        end

        # Set or get the finalize function for the operation.
        #
        # @example Set the finalize function.
        #   map_reduce.finalize(function)
        #
        # @param [ String ] function The finalize js function.
        #
        # @return [ MapReduce, String ] The new MapReduce operation or the
        #   value of the function.
        #
        # @since 2.0.0
        def finalize(function = nil)
          configure(:finalize, function)
        end

        # Initialize the map/reduce for the provided collection view, functions
        # and options.
        #
        # @example Create the new map/reduce view.
        #
        # @param [ Collection::View ] view The collection view.
        # @param [ String ] map The map function.
        # @param [ String ] reduce The reduce function.
        # @param [ Hash ] options The map/reduce options.
        #
        # @since 2.0.0
        def initialize(view, map, reduce, options = {})
          @view = view
          @map_function = map.dup.freeze
          @reduce_function = reduce.dup.freeze
          @options = BSON::Document.new(options).freeze

          client.log_warn('The map_reduce operation is deprecated, please use the aggregation pipeline instead')
        end

        # Set or get the jsMode flag for the operation.
        #
        # @example Set js mode for the operation.
        #   map_reduce.js_mode(true)
        #
        # @param [ true, false ] value The jsMode value.
        #
        # @return [ MapReduce, true, false ] The new MapReduce operation or the
        #   value of the jsMode flag.
        #
        # @since 2.0.0
        def js_mode(value = nil)
          configure(:js_mode, value)
        end

        # Set or get the output location for the operation.
        #
        # @example Set the output to inline.
        #   map_reduce.out(inline: 1)
        #
        # @example Set the output collection to merge.
        #   map_reduce.out(merge: 'users')
        #
        # @example Set the output collection to replace.
        #   map_reduce.out(replace: 'users')
        #
        # @example Set the output collection to reduce.
        #   map_reduce.out(reduce: 'users')
        #
        # @param [ Hash ] location The output location details.
        #
        # @return [ MapReduce, Hash ] The new MapReduce operation or the value
        #   of the output location.
        #
        # @since 2.0.0
        def out(location = nil)
          configure(:out, location)
        end

        # Returns the collection name where the map-reduce result is written to.
        # If the result is returned inline, returns nil.
        def out_collection_name
          if options[:out].respond_to?(:keys)
            options[:out][OUT_ACTIONS.find do |action|
              options[:out][action]
            end]
          end || options[:out]
        end

        # Returns the database name where the map-reduce result is written to.
        # If the result is returned inline, returns nil.
        def out_database_name
          if options[:out]
            if options[:out].respond_to?(:keys) && (db = options[:out][:db])
              db
            else
              database.name
            end
          end
        end

        # Set or get a scope on the operation.
        #
        # @example Set the scope value.
        #   map_reduce.scope(value: 'test')
        #
        # @param [ Hash ] object The scope object.
        #
        # @return [ MapReduce, Hash ] The new MapReduce operation or the value
        #   of the scope.
        #
        # @since 2.0.0
        def scope(object = nil)
          configure(:scope, object)
        end

        # Whether to include the timing information in the result.
        #
        # @example Set the verbose value.
        #   map_reduce.verbose(false)
        #
        # @param [ true, false ] value Whether to include timing information
        #   in the result.
        #
        # @return [ MapReduce, Hash ] The new MapReduce operation or the value
        #   of the verbose option.
        #
        # @since 2.0.5
        def verbose(value = nil)
          configure(:verbose, value)
        end

        # Execute the map reduce, without doing a fetch query to retrieve the results
        #   if outputted to a collection.
        #
        # @example Execute the map reduce and get the raw result.
        #   map_reduce.execute
        #
        # @return [ Mongo::Operation::Result ] The raw map reduce result
        #
        # @since 2.5.0
        def execute
          view.send(:with_session, @options) do |session|
            write_concern = view.write_concern_with_session(session)
            context = Operation::Context.new(client: client, session: session)
            nro_write_with_retry(write_concern, context: context) do |connection, txn_num, context|
              send_initial_query_with_connection(connection, session, context: context)
            end
          end
        end

        private

        OUT_ACTIONS = [ :replace, :merge, :reduce ].freeze

        def server_selector
          @view.send(:server_selector)
        end

        def inline?
          out.nil? || out == { inline: 1 } || out == { INLINE => 1 }
        end

        def map_reduce_spec(session = nil)
          Builder::MapReduce.new(map_function, reduce_function, view, options.merge(session: session)).specification
        end

        def new(options)
          MapReduce.new(view, map_function, reduce_function, options)
        end

        def initial_query_op(session)
          spec = map_reduce_spec(session)
          # Read preference isn't simply passed in the command payload
          # (it may need to be converted to wire protocol flags).
          # Passing it in command payload produces errors on at least
          # 5.0 mongoses.
          # In the future map_reduce_command should remove :read
          # from its return value, however we cannot do this right now
          # due to Mongoid 7 relying on :read being returned as part of
          # the command - see RUBY-2932.
          # Delete :read here for now because it cannot be sent to mongos this way.
          spec = spec.dup
          spec[:selector] = spec[:selector].dup
          spec[:selector].delete(:read)
          Operation::MapReduce.new(spec)
        end

        def valid_server?(description)
          if secondary_ok?
            true
          else
            description.standalone? || description.mongos? || description.primary? || description.load_balancer?
          end
        end

        def secondary_ok?
          out.respond_to?(:keys) && out.keys.first.to_s.downcase == INLINE
        end

        def send_initial_query(server, session, context:)
          server.with_connection do |connection|
            send_initial_query_with_connection(connection, session, context: context)
          end
        end

        def send_initial_query_with_connection(connection, session, context:)
          op = initial_query_op(session)
          if valid_server?(connection.description)
            op.execute_with_connection(connection, context: context)
          else
            msg = "Rerouting the MapReduce operation to the primary server - #{connection.address} is not suitable because it is not currently the primray"
            log_warn(msg)
            server = cluster.next_primary(nil, session)
            op.execute(server, context: context)
          end
        end

        def fetch_query_spec
          Builder::MapReduce.new(map_function, reduce_function, view, options).query_specification
        end

        def find_command_spec(session)
          Builder::MapReduce.new(map_function, reduce_function, view, options.merge(session: session)).command_specification
        end

        def fetch_query_op(server, session)
          spec = {
            coll_name: out_collection_name,
            db_name: out_database_name,
            filter: {},
            session: session,
            read: read,
            read_concern: options[:read_concern] || collection.read_concern,
            collation: options[:collation] || view.options[:collation],
          }
          Operation::Find.new(spec)
        end

        def send_fetch_query(server, session)
          fetch_query_op(server, session).execute(server, context: Operation::Context.new(client: client, session: session))
        end
      end
    end
  end
end
