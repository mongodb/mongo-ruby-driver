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
        class Insert

          # Defines custom behaviour of results when inserting.
          #
          # @since 2.0.0
          class Result < Operation::Result
            include Mergable

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

          # Defines custom behaviour of results when inserting.
          # For server versions < 2.5.5 (that don't use write commands).
          #
          # @since 2.0.0
          class LegacyResult < Operation::Result
            include LegacyMergable

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
                n += 1 unless reply_write_errors?(reply)
                n
              end
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
end
