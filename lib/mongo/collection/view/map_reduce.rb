# Copyright (C) 2014-2019 MongoDB, Inc.
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
          result = send_initial_query(server, session)
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
            legacy_write_with_retry do |server|
              send_initial_query(server, session)
            end
          end
        end

        private

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
          Operation::MapReduce.new(map_reduce_spec(session))
        end

        def valid_server?(server)
          server.standalone? || server.mongos? || server.primary? || secondary_ok?
        end

        def secondary_ok?
          out.respond_to?(:keys) && out.keys.first.to_s.downcase == INLINE
        end

        def send_initial_query(server, session)
          unless valid_server?(server)
            log_warn("Rerouting the MapReduce operation to the primary server - #{server.summary} is not suitable")
            server = cluster.next_primary(nil, session)
          end
          validate_collation!(server)
          initial_query_op(session).execute(server)
        end

        def fetch_query_spec
          Builder::MapReduce.new(map_function, reduce_function, view, options).query_specification
        end

        def find_command_spec(session)
          Builder::MapReduce.new(map_function, reduce_function, view, options.merge(session: session)).command_specification
        end

        def fetch_query_op(server, session)
          if server.features.find_command_enabled?
            Operation::Find.new(find_command_spec(session))
          else
            Operation::Find.new(fetch_query_spec)
          end
        end

        def send_fetch_query(server, session)
          fetch_query_op(server, session).execute(server)
        end

        def validate_collation!(server)
          if (view.options[:collation] || options[:collation]) && !server.features.collation_enabled?
            raise Error::UnsupportedCollation.new
          end
        end
      end
    end
  end
end
