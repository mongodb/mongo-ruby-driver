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

      # Provides behaviour around a map/reduce operation on the collection
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
        REROUTE = 'Rerouting the MapReduce operation to the primary server.'.freeze

        # @return [ View ] view The collection view.
        attr_reader :view

        # @return [ String ] map The map function.
        attr_reader :map

        # @return [ String ] reduce The reduce function.
        attr_reader :reduce

        # Delegate necessary operations to the view.
        def_delegators :view, :collection, :read, :cluster

        # Delegate necessary operations to the collection.
        def_delegators :collection, :database

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
          write_with_retry do
            server = read.select_server(cluster, false)
            result = send_initial_query(server)
            @cursor = Cursor.new(view, result, server)
          end
          @cursor.each do |doc|
            yield doc
          end if block_given?
          @cursor.to_enum
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
          @map = map.freeze
          @reduce = reduce.freeze
          @options = options.freeze
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

        private

        def inline?
          out.nil? || out == { inline: 1 } || out == { INLINE => 1 }
        end

        def map_reduce_spec
          Builder::MapReduce.new(map, reduce, view, options).specification
        end

        def new(options)
          MapReduce.new(view, map, reduce, options)
        end

        def initial_query_op
          Operation::Commands::MapReduce.new(map_reduce_spec)
        end

        def valid_server?(server)
          server.standalone? || server.mongos? || server.primary? || secondary_ok?
        end

        def secondary_ok?
          out.respond_to?(:keys) && out.keys.first.to_s.downcase == INLINE
        end

        def send_initial_query(server)
          unless valid_server?(server)
            log_warn(REROUTE)
            server = cluster.next_primary(false)
          end
          result = initial_query_op.execute(server.context)
          inline? ? result : send_fetch_query(server)
        end

        def fetch_query_spec
          Builder::MapReduce.new(map, reduce, view, options).query_specification
        end

        def find_command_spec
          Builder::MapReduce.new(map, reduce, view, options).command_specification
        end

        def fetch_query_op(server)
          if server.features.find_command_enabled?
            Operation::Commands::Find.new(find_command_spec)
          else
            Operation::Read::Query.new(fetch_query_spec)
          end
        end

        def send_fetch_query(server)
          fetch_query_op(server).execute(server.context)
        end
      end
    end
  end
end
