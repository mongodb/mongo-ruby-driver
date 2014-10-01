# Copyright (C) 2009-2014 MongoDB, Inc.
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
      class BulkInsert
  
        # Defines custom behaviour of results when inserting.
        #
        # @since 2.0.0
        class Result < Operation::Result

          # Gets the number of documents inserted.
          #
          # @example Get the number of documents inserted.
          #   result.n_inserted
          #
          # @return [ Integer ] The number of documents inserted.
          #
          # @since 2.0.0
          def n_inserted
            written_count
          end
        end

        # Defines custom behaviour of results when inserting.
        # For server versions < 2.5.5 (that don't use write commands).
        #
        # @since 2.0.0
        class LegacyResult < Operation::Result

          # Gets the number of documents inserted.
          #
          # @example Get the number of documents inserted.
          #   result.n_inserted
          #
          # @return [ Integer ] The number of documents inserted.
          #
          # @since 2.0.0
          def n_inserted
            return 0 unless acknowledged?
            @replies.reduce(0) do |n, reply|
              n += 1 if reply.documents.first[OK] == 1
              n
            end
          end
        end
      end
    end
  end
end
