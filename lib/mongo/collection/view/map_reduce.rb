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
        include Iterable

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
          out.nil? || out == { inline: 1 } || out == { 'inline' => 1 }
        end

        def map_reduce_spec
          {
            :db_name => database.name,
            :read => read,
            :selector => {
              :mapreduce => collection.name,
              :map => map,
              :reduce => reduce,
              :query => view.selector[:$query] || view.selector,
              :out => { inline: 1 }
            }.merge(options).merge(view.options)
          }
        end

        def new(options)
          MapReduce.new(view, map, reduce, options)
        end

        def initial_query_op
          Operation::MapReduce.new(map_reduce_spec)
        end

        def send_initial_query(server)
          result =
            begin
              initial_query_op.execute(server.context)
            rescue Mongo::Error::NeedPrimaryServer
              warn 'Rerouting the MapReduce operation to the primary server.'
              server = ServerSelector.get(mode: :primary).select_server(cluster)
              initial_query_op.execute(server.context)
            end
          if inline?
            result
          else
            send_fetch_query(server)
          end
        end

        def fetch_query_spec
          { :selector  => {},
            :options   => {},
            :db_name   => database.name,
            :coll_name => out.values.first }
        end

        def fetch_query_op
          Operation::Read::Query.new(fetch_query_spec)
        end

        def send_fetch_query(server)
          fetch_query_op.execute(server.context)
        end
      end
    end
  end
end
