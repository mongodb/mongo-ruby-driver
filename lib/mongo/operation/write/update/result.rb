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
      class Update

        # Defines custom behaviour of results for an update.
        #
        # @since 2.0.0
        class Result < Operation::Result

          # The number of modified docs field in the result.
          #
          # @since 2.0.0
          MODIFIED = 'nModified'.freeze

          # The upserted docs field in the result.
          #
          # @since 2.0.0
          UPSERTED = 'upserted'.freeze

          # Get the number of documents matched.
          #
          # @example Get the matched count.
          #   result.matched_count
          #
          # @return [ Integer ] The matched count.
          #
          # @since 2.0.0
          def matched_count
            return 0 unless acknowledged?
            if upsert?
              0
            else
              n
            end
          end

          # Get the number of documents modified.
          #
          # @example Get the modified count.
          #   result.modified_count
          #
          # @return [ Integer ] The modified count.
          #
          # @since 2.0.0
          def modified_count
            return 0 unless acknowledged?
            first[MODIFIED]
          end

          # The identifier of the inserted document if an upsert
          #   took place.
          #
          # @example Get the upserted document's identifier.
          #   result.upserted_id
          #
          # @return [ Object ] The upserted id.
          #
          # @since 2.0.0
          def upserted_id
            return nil unless upsert?
            upsert?.first['_id']
          end

          # Returns the number of documents upserted.
          #
          # @example Get the number of upserted documents.
          #   result.upserted_count
          #
          # @return [ Integer ] The number upserted.
          #
          # @since 2.4.2
          def upserted_count
            upsert? ? n : 0
          end

          private

          def upsert?
            first[UPSERTED]
          end
        end

        # Defines custom behaviour of results for an update on server
        # version <= 2.4.
        #
        # @since 2.0.0
        class LegacyResult < Operation::Result

          # Whether an existing document was updated.
          #
          # @since 2.0.0
          UPDATED_EXISTING = 'updatedExisting'.freeze

          # The upserted docs field in the result.
          #
          # @since 2.0.0
          UPSERTED = 'upserted'.freeze

          # Get the number of documents matched.
          #
          # @example Get the matched count.
          #   result.matched_count
          #
          # @return [ Integer ] The matched count.
          #
          # @since 2.0.0
          def matched_count
            return 0 unless acknowledged?
            if upsert?
              0
            else
              n
            end
          end

          # Get the number of documents modified.
          #
          # @example Get the modified count.
          #   result.modified_count
          #
          # @return [ nil ] Always omitted for legacy versions.
          #
          # @since 2.0.0
          def modified_count; end

          # The identifier of the inserted document if an upsert
          #   took place.
          #
          # @example Get the upserted document's identifier.
          #   result.upserted_id
          #
          # @return [ Object ] The upserted id.
          #
          # @since 2.0.0
          def upserted_id
            first[UPSERTED] if upsert?
          end

          # Returns the number of documents upserted.
          #
          # @example Get the number of upserted documents.
          #   result.upserted_count
          #
          # @return [ Integer ] The number upserted.
          #
          # @since 2.4.2
          def upserted_count
            upsert? ? n : 0
          end

          private

          def upsert?
            !updated_existing? && n == 1
          end

          def updated_existing?
            first[UPDATED_EXISTING]
          end
        end
      end
    end
  end
end
