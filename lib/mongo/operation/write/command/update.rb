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

        # A MongoDB update write command operation.
        #
        # @example Create an update write command operation.
        #   Write::Command::Update.new({
        #     :updates => [{
        #       :q => { :foo => 1 },
        #       :u => { :$set =>
        #       :bar => 1 }},
        #       :multi  => true,
        #       :upsert => false
        #       :array_filters => []
        #     }],
        #     :db_name => 'test',
        #     :coll_name => 'test_coll',
        #     :write_concern => write_concern,
        #     :ordered => true,
        #     :bypass_document_validation => true
        #   })
        #
        # @since 2.0.0
        class Update
          include Specifiable
          include Writable

          private

          IDENTIFIER = 'updates'.freeze

          def selector
            { update: coll_name,
              updates: updates
            }.merge(command_options)
          end

          def op_msg(server)
            global_args = { update: coll_name,
                            Protocol::Msg::DATABASE_IDENTIFIER => db_name
                          }.merge!(command_options)
            update_selector_for_session!(global_args, server)

            section = { type: 1, payload: { identifier: IDENTIFIER, sequence: updates } }
            flags = unacknowledged_write? ? [:more_to_come] : [:none]
            Protocol::Msg.new(flags, {}, global_args, section)
          end

          def message(server)
            if server.features.op_msg_enabled?
              op_msg(server)
            else
              Protocol::Query.new(db_name, Database::COMMAND, selector, options)
            end
          end
        end
      end
    end
  end
end

