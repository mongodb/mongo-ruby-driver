# Copyright (C) 2009-2014 MongoDB, Inc.
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

        # Iterator over the results of the aggregation.
        #
        # @example Iterate over the results.
        #   aggregation.each do |doc|
        #     p doc
        #   end
        #
        # @yieldparam [ BSON::Document ] Each returned document.
        #
        # @return [ Enumerator ] The enumerator.
        #
        # @since 2.0.0
        def each
          server = read.select_servers(cluster.servers).first
          cursor = Cursor.new(view, send_initial_query(server), server).to_enum
          if block_given?
            cursor.each{ |document| yield document }
          end
          cursor
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
          @options = options.dup
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
          configure(:jsMode, value)
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
        # @return [ MapReduce, Hash ] The new MapReduce operation or thevalue
        #   of the scope.
        #
        # @since 2.0.0
        def scope(object = nil)
          configure(:scope, object)
        end

        private

        def map_reduce_spec
          {
            :db_name => database.name,
            :selector => {
              :mapreduce => collection.name,
              :map => map,
              :reduce => reduce,
              :query => view.selector,
              :out => { inline: 1 }
            }
          }.merge(options)
        end

        def new(options)
          MapReduce.new(view, map, reduce, options)
        end

        def initial_query_op
          Operation::MapReduce.new(map_reduce_spec)
        end

        def send_initial_query(server)
          # Send the initial map/reduce
          initial_query_op.execute(server.context)
          # If an output collection was specified, then execute the query.
        end
      end
    end
  end
end
