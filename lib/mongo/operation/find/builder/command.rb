# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2015-2020 MongoDB Inc.
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
  module Operation
    class Find
      module Builder

        # Builds a find command specification from options.
        #
        # @api private
        module Command

          # The mappings from ruby options to the find command.
          OPTION_MAPPINGS = BSON::Document.new(
            allow_disk_use: 'allowDiskUse',
            allow_partial_results: 'allowPartialResults',
            await_data: 'awaitData',
            batch_size: 'batchSize',
            collation: 'collation',
            comment: 'comment',
            filter: 'filter',
            hint: 'hint',
            let: 'let',
            limit: 'limit',
            max_scan: 'maxScan',
            max_time_ms: 'maxTimeMS',
            max_value: 'max',
            min_value: 'min',
            no_cursor_timeout: 'noCursorTimeout',
            oplog_replay: 'oplogReplay',
            projection: 'projection',
            read_concern: 'readConcern',
            return_key: 'returnKey',
            show_disk_loc: 'showRecordId',
            single_batch: 'singleBatch',
            skip: 'skip',
            snapshot: 'snapshot',
            sort: 'sort',
            tailable: 'tailable',
            tailable_cursor: 'tailable',
          ).freeze

          module_function def selector(spec, connection)
            if spec[:collation] && !connection.features.collation_enabled?
              raise Error::UnsupportedCollation
            end

            BSON::Document.new.tap do |selector|
              OPTION_MAPPINGS.each do |k, server_k|
                unless (value = spec[k]).nil?
                  selector[server_k] = value
                end
              end

              if rc = selector[:readConcern]
                selector[:readConcern] = Options::Mapper.transform_values_to_strings(rc)
              end

              convert_limit_and_batch_size!(selector)
            end
          end

          private

          # Converts negative limit and batchSize parameters in the
          # find command to positive ones. Removes the parameters if their
          # values are zero.
          #
          # This is only used for find commmand, not for OP_QUERY path.
          #
          # The +command+ parameter is mutated by this method.
          module_function def convert_limit_and_batch_size!(command)
            if command[:limit] && command[:limit] < 0 &&
              command[:batchSize] && command[:batchSize] < 0
            then
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
        end
      end
    end
  end
end
