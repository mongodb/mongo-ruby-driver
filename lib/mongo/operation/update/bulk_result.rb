# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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
    class Update

      # Defines custom behavior of results for an udpate when sent as part of a bulk write.
      #
      # @since 2.0.0
      class BulkResult < Operation::Result
        include Aggregatable

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
              n += reply.documents.first[UPSERTED].size
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
              reply.documents.first[N] - n_upserted
            else
              if reply.documents.first[N]
                n += reply.documents.first[N]
              else
                n
              end
            end
          end
        end

        # Gets the number of documents modified.
        # Not that in a mixed sharded cluster a call to
        # update could return nModified (>= 2.6) or not (<= 2.4).
        # If any call does not return nModified we can't report
        # a valid final count so set the field to nil.
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
            if n && reply.documents.first[MODIFIED]
              n += reply.documents.first[MODIFIED]
            else
              0
            end
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
          return [] unless acknowledged?
          @replies.reduce([]) do |ids, reply|
            if upserted_ids = reply.documents.first[UPSERTED]
              ids += upserted_ids
            end
            ids
          end
        end

        private

        def upsert?(reply)
          upserted.any?
        end
      end
    end
  end
end
