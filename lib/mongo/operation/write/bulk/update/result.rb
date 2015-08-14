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
      module Bulk
        class Update

          # Defines custom behaviour of results when updating.
          #
          # @since 2.0.0
          class Result < Operation::Result
            include Mergable

            # The number of modified docs field in the result.
            #
            # @since 2.0.0
            MODIFIED = 'nModified'.freeze

            # The upserted docs field in the result.
            #
            # @since 2.0.0
            UPSERTED = 'upserted'.freeze

            # Gets the number of documents upserted.
            #
            # @example Get the upserted count.
            #   result.n_upserted
            #
            # @return [ Integer ] The number of documents upserted.
            #
            # @since 2.0.0
            def n_upserted
              return 0 unless acknowledged?
              @replies.reduce(0) do |n, reply|
                if upsert?(reply)
                  n += 1
                else
                  n += 0
                end
              end
            end

            # Gets the number of documents matched.
            #
            # @example Get the matched count.
            #   result.n_matched
            #
            # @return [ Integer ] The number of documents matched.
            #
            # @since 2.0.0
            def n_matched
              return 0 unless acknowledged?
              @replies.reduce(0) do |n, reply|
                if upsert?(reply)
                  n += 0
                else
                  n += reply.documents.first[N]
                end
              end
            end

            # Gets the number of documents modified.
            #
            # @example Get the modified count.
            #   result.n_modified
            #
            # @return [ Integer ] The number of documents modified.
            #
            # @since 2.0.0
            def n_modified
              return 0 unless acknowledged?
              @replies.reduce(0) do |n, reply|
                n += reply.documents.first[MODIFIED] || 0
              end
            end

            # Get the upserted documents.
            #
            # @example Get upserted documents.
            #   result.upserted
            #
            # @return [ Array<BSON::Document> ] The upserted document info
            #
            # @since 2.1.0
            def upserted
              reply.documents.first[UPSERTED] || []
            end

            private

            def upsert?(reply)
              upserted.any?
            end
          end

          # Defines custom behaviour of results when updating.
          # For server versions < 2.5.5 (that don't use write commands).
          #
          # @since 2.0.0
          class LegacyResult < Operation::Result
            include LegacyMergable

            # The updated existing field in the result.
            #
            # @since 2.0.0
            UPDATED_EXISTING = 'updatedExisting'.freeze

            # Gets the number of documents upserted.
            #
            # @example Get the upserted count.
            #   result.n_upserted
            #
            # @return [ Integer ] The number of documents upserted.
            #
            # @since 2.0.0
            def n_upserted
              return 0 unless acknowledged?
              @replies.reduce(0) do |n, reply|
                if upsert?(reply)
                  n += reply.documents.first[N]
                else
                  n
                end
              end
            end

            # Gets the number of documents matched.
            #
            # @example Get the matched count.
            #   result.n_matched
            #
            # @return [ Integer ] The number of documents matched.
            #
            # @since 2.0.0
            def n_matched
              return 0 unless acknowledged?
              @replies.reduce(0) do |n, reply|
                if upsert?(reply)
                  n
                else
                  n += reply.documents.first[N]
                end
              end
            end
            alias :n_modified :n_matched

            private

            def upsert?(reply)
              !updated_existing?(reply) && reply.documents.first[N] == 1
            end

            def updated_existing?(reply)
              reply.documents.first[UPDATED_EXISTING]
            end
          end
        end
      end
    end
  end
end
