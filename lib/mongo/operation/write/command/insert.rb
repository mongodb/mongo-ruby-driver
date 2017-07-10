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

          # The query selector for this insert command operation.
          #
          # @return [ Hash ] The selector describing this insert operation.
          #
          # @since 2.0.0
          def selector
            { insert: coll_name,
              documents: documents,
              ordered: ordered?
            }.tap do |cmd|
              cmd.merge!(writeConcern: write_concern.options) if write_concern
              cmd.merge!(:bypassDocumentValidation => true) if bypass_document_validation
            end
          end

          # The wire protocol message for this write operation.
          #
          # @return [ Mongo::Protocol::Query ] Wire protocol message.
          #
          # @since 2.2.5
          def message(server)
            opts = options.merge(validating_keys: true)
            Protocol::Query.new(db_name, Database::COMMAND, selector, opts)
          end
        end
      end
    end
  end
end

