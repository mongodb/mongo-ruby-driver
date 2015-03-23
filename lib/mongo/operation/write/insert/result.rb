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
        # @since 2.0.0
        class Result < Operation::Result

          # Get the inserted ids.
          #
          # @example Get the inserted ids.
          #   result.inserted_ids
          #
          # @return [ Array ] The inserted ids.
          #
          # @since 2.0.0
          def inserted_ids
            documents
          end

          # Get the inserted id.
          #
          # @example Get the inserted id.
          #   result.inserted_id
          #
          # @return [ Object ] The inserted id.
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
