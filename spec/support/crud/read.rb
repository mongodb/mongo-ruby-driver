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
  module CRUD
    module Operation

      # Defines common behaviour for running CRUD read operation tests
      # on a collection.
      #
      # @since 2.0.0
      class Read

        # Map of method names to test operation argument names.
        #
        # @since 2.0.0
        ARGUMENT_MAP = { :sort => 'sort',
                         :skip => 'skip',
                         :batch_size => 'batch_size',
                         :limit => 'limit'
                       }

        # The operation name.
        #
        # @return [ String ] name The operation name.
        #
        # @since 2.0.0
        attr_reader :name
  
        # Instantiate the operation.
        #
        # @return [ Hash ] spec The operation spec.
        #
        # @since 2.0.0
        def initialize(spec)
          @spec = spec
          @name = spec['name']
        end

        # Execute the operation.
        #
        # @example Execute the operation.
        #   operation.execute
        #
        # @param [ Collection ] collection The collection to execute the operation on.
        #
        # @return [ Result, Array<Hash> ] The result of executing the operation.
        #
        # @since 2.0.0
        def execute(collection)
          send(name.to_sym, collection)
        end

        # Whether the operation is expected to have restuls.
        #
        # @example Whether the operation is expected to have results.
        #   operation.has_results?
        #
        # @return [ true, false ] If the operation is expected to have results.
        #
        # @since 2.0.0
        def has_results?
          !(name == 'aggregate' &&
              pipeline.find {|op| op.keys.include?('$out') })
        end

        # Whether the operation requires server version >= 2.6 for its results to
        # match expected results.
        #
        # @example Whether the operation requires >= 2.6 for its results to match.
        #   operation.requires_2_6?(collection)
        #
        # @param [ Collection ] collection The collection the operation is executed on.
        #
        # @return [ true, false ] If the operation requires 2.6 for its results to match.
        #
        # @since 2.0.0
        def requires_2_6?(collection)
          name == 'aggregate' && pipeline.find {|op| op.keys.include?('$out') }
        end

        private
  
        def count(collection)
          view = collection.find(filter)
          options = ARGUMENT_MAP.reduce({}) do |opts, (key, value)|
            opts.merge!(key => arguments[value]) if arguments[value]
            opts
          end
          view.count(options)
        end
  
        def aggregate(collection)
          collection.find.tap do |view|
            view = view.batch_size(batch_size) if batch_size
          end.aggregate(pipeline).to_a
        end
  
        def distinct(collection)
          collection.find(filter).distinct(field_name)
        end
  
        def find(collection)
          view = collection.find(filter)
          ARGUMENT_MAP.each do |key, value|
            view = view.send(key, arguments[value]) if arguments[value]
          end
          view.to_a
        end
  
        def batch_size
          arguments['batchSize']
        end

        def filter
          arguments['filter']
        end
  
        def pipeline
          arguments['pipeline']
        end

        def field_name
          arguments['fieldName']
        end

        def arguments
          @spec['arguments']
        end
      end
    end
  end
end