# Copyright (C) 2015-2017 MongoDB, Inc.
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
      module Builder

        # Builds a find command specification from options.
        #
        # @since 2.2.0
        class FindCommand
          extend Forwardable

          # The mappings from ruby options to the find command.
          #
          # @since 2.2.0
          MAPPINGS = BSON::Document.new(
            sort: 'sort',
            projection: 'projection',
            hint: 'hint',
            skip: 'skip',
            limit: 'limit',
            batch_size: 'batchSize',
            single_batch: 'singleBatch',
            comment: 'comment',
            max_scan: 'maxScan',
            max_time_ms: 'maxTimeMS',
            max_value: 'max',
            min_value: 'min',
            return_key: 'returnKey',
            show_disk_loc: 'showRecordId',
            snapshot: 'snapshot',
            tailable: 'tailable',
            tailable_cursor: 'tailable',
            oplog_replay: 'oplogReplay',
            no_cursor_timeout: 'noCursorTimeout',
            await_data: 'awaitData',
            allow_partial_results: 'allowPartialResults',
            read_concern: 'readConcern',
            collation: 'collation'
          ).freeze

          def_delegators :@view, :collection, :database, :filter, :options, :read

          # Get the specification for an explain command that wraps the find
          # command.
          #
          # @example Get the explain spec.
          #   builder.explain_specification
          #
          # @return [ Hash ] The specification.
          #
          # @since 2.2.0
          def explain_specification
            { selector: { explain: find_command }, db_name: database.name, read: read }
          end

          # Create the find command builder.
          #
          # @example Create the find command builder.
          #   FindCommandBuilder.new(view)
          #
          # @param [ Collection::View ] view The collection view.
          #
          # @since 2.2.2
          def initialize(view)
            @view = view
          end

          # Get the specification to pass to the find command operation.
          #
          # @example Get the specification.
          #   builder.specification
          #
          # @return [ Hash ] The specification.
          #
          # @since 2.2.0
          def specification
            { selector: find_command, db_name: database.name, read: read }
          end

          private

          def find_command
            document = BSON::Document.new('find' => collection.name, 'filter' => filter)
            command = Options::Mapper.transform_documents(convert_flags(options), MAPPINGS, document)
            convert_limit_and_batch_size(command)
            command
          end

          def convert_limit_and_batch_size(command)
            if command[:limit] && command[:limit] < 0 &&
                command[:batchSize] && command[:batchSize] < 0

              command[:limit] = command[:limit].abs
              command[:batchSize] = command[:limit].abs
              command[:singleBatch] = true

            else
              [:limit, :batchSize].each do |opt|
                if command[opt]
                  if command[opt] < 0
                    command[opt] = command[opt].abs
                    command[:singleBatch] = true
                  elsif command[opt] == 0
                    command.delete(opt)
                  end
                end
              end
            end
          end

          def convert_flags(options)
            return options if options.empty?
            opts = options.dup
            opts.delete(:cursor_type)
            Flags.map_flags(options).reduce(opts) do |o, key|
              o.merge!(key => true)
            end
          end
        end
      end
    end
  end
end
