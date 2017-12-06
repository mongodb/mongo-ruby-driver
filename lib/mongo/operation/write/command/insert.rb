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
  module Operation
    module Write
      module Command

        # A MongoDB insert write command operation.
        #
        # @example Create an insert write command operation.
        #   Write::Command::Insert.new({
        #     :documents => [{ :foo => 1 }],
        #     :db_name => 'test',
        #     :coll_name => 'test_coll',
        #     :write_concern => write_concern,
        #     :ordered => true
        #   })
        # @since 2.0.0
        class Insert
          include Specifiable
          include Writable

          private

          IDENTIFIER = 'documents'.freeze

          def selector
            { insert: coll_name,
              documents: documents
            }.merge!(command_options)
          end

          def op_msg(server)
            global_args = { insert: coll_name,
                            Protocol::Msg::DATABASE_IDENTIFIER => db_name
                          }.merge!(command_options)
            update_selector_for_session!(global_args, server)

            section = { type: 1, payload: { identifier: IDENTIFIER, sequence: documents } }
            flags = unacknowledged_write? ? [:more_to_come] : [:none]
            Protocol::Msg.new(flags, { validating_keys: true }, global_args, section)
          end

          def message(server)
            if server.features.op_msg_enabled?
              op_msg(server)
            else
              opts = options.merge(validating_keys: true)
              Protocol::Query.new(db_name, Database::COMMAND, selector, opts)
            end
          end
        end
      end
    end
  end
end

