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

        # A MongoDB ensure index write command operation.
        #
        # @example Create an ensure index command operation.
        #   Write::Command::CreateIndex.new({
        #     :indexes => [{ :key => { :foo => 1 }, :name => 'foo_1', :unique => true }],
        #     :db_name => 'test',
        #     :coll_name => 'test_coll'
        #   })
        #
        # @since 2.0.0
        class CreateIndex
          include Specifiable
          include Writable
          include TakesWriteConcern

          private

          # The query selector for this ensure index command operation.
          #
          # @return [ Hash ] The selector describing this insert operation.
          #
          # @since 2.0.0
          def selector
            { :createIndexes => coll_name, :indexes => indexes }
          end

          def message(server)
            sel = update_selector_for_write_concern(selector, server)
            Protocol::Query.new(db_name, Database::COMMAND, sel, options)
          end
        end
      end
    end
  end
end

