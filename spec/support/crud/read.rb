# Copyright (C) 2014-2017 MongoDB, Inc.
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
                         :batch_size => 'batchSize',
                         :limit => 'limit',
                         :collation => 'collation'
                       }.freeze

        # Map of read preference mode names to their equivalent Ruby-formatted symbols.
        #
        # @since 2.4.0
        READ_PREFERENCE_MAP = { 'primary' => :primary,
                                'secondary' => :secondary,
                                'primaryPreferred' => :primary_preferred,
                                'secondaryPreferred' => :secondary_preferred,
                                'nearest' => :nearest
                              }.freeze

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

        private

        def count(collection)
          options = ARGUMENT_MAP.reduce({}) do |opts, (key, value)|
            opts.merge!(key => arguments[value]) if arguments[value]
            opts
          end
          collection.count(filter, options)
        end

        def aggregate(collection)
          collection.aggregate(pipeline, options).to_a
        end

        def distinct(collection)
          collection.distinct(field_name, filter, options)
        end

        def find(collection)
          opts = modifiers ? options.merge(modifiers: BSON::Document.new(modifiers)) : options
          (read_preference ? collection.with(read: read_preference) : collection).find(filter, opts).to_a
        end

        def options
          ARGUMENT_MAP.reduce({}) do |opts, (key, value)|
            value = if arguments[value].is_a?(Hash) && arguments[value]['$numberLong']
                      arguments[value]['$numberLong'].to_i
                    else
                      arguments[value]
                    end
            value ? opts.merge!(key => value) : opts
          end
        end

        def collation
          arguments['collation']
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

        def modifiers
          arguments['modifiers']
        end

        def field_name
          arguments['fieldName']
        end

        def arguments
          @spec['arguments']
        end

        def read_preference
          if @spec['read_preference'] && @spec['read_preference']['mode']
            { mode: READ_PREFERENCE_MAP[@spec['read_preference']['mode']] }
          end
        end
      end
    end
  end
end
