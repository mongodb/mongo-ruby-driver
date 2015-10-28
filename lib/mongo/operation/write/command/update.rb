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

          # The query selector for this update command operation.
          #
          # @return [ Hash ] The selector describing this update operation.
          def selector
            { update: coll_name,
              updates: updates,
              ordered: ordered?
            }.tap do |cmd|
              cmd.merge!(writeConcern: write_concern.options) if write_concern
              cmd.merge!(:bypassDocumentValidation => true) if bypass_document_validation
            end
          end
        end
      end
    end
  end
end

