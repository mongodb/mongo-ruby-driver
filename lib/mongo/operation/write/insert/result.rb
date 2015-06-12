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
      class Insert

        # Defines custom behaviour of results for an insert.
        #
        # According to the CRUD spec, reporting the inserted ids
        # is optional. It can be added to this class later, if needed.
        #
        # @since 2.0.0
        class Result < Operation::Result

          # Get the ids of the inserted documents.
          #
          # @since 2.0.0
          attr_reader :inserted_ids

          # Initialize a new result.
          #
          # @example Instantiate the result.
          #   Result.new(replies, inserted_ids)
          #
          # @param [ Protocol::Reply ] replies The wire protocol replies.
          # @param [ Array<Object> ] ids The ids of the inserted documents.
          #
          # @since 2.0.0
          def initialize(replies, ids)
            @replies = replies.is_a?(Protocol::Reply) ? [ replies ] : replies
            @inserted_ids = ids
          end

          # Gets the id of the document inserted.
          #
          # @example Get id of the document inserted.
          #   result.inserted_id
          #
          # @return [ Object ] The id of the document inserted.
          #
          # @since 2.0.0
          def inserted_id
            inserted_ids.first
          end
        end
      end
    end
  end
end
