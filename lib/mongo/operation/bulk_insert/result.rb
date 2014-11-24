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

          attr_reader :indexes

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

          # Set a list of indexes of the operations creating this result.
          #
          # @example Set the list of indexes.
          #   result.set_indexes([1,2,3])
          #
          # @return [ self ] The result.
          #
          # @since 2.0.0
          def set_indexes(indexes)
            @indexes = indexes
            self
          end

          # Aggregate the write errors returned from this result.
          #
          # @example Aggregate the write errors.
          #   result.aggregate_write_errors
          #
          # @return [ Array ] The aggregate write errors.
          #
          # @since 2.0.0
          def aggregate_write_errors
            @replies.reduce([]) do |all_write_errors, reply|
              if write_errors = reply.documents.first['writeErrors']
                write_errors.each do |write_error|
                  all_write_errors << write_error.merge('index' => indexes[write_error['index']])
                end
                all_write_errors
              end
            end
          end
        end

        # Defines custom behaviour of results when inserting.
        # For server versions < 2.5.5 (that don't use write commands).
        #
        # @since 2.0.0
        class LegacyResult < Operation::Result

          attr_reader :indexes

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

          # Set a list of indexes of the operations creating this result.
          #
          # @example Set the list of indexes.
          #   result.set_indexes([1,2,3])
          #
          # @return [ self ] The result.
          #
          # @since 2.0.0
          def set_indexes(indexes)
            @indexes = indexes
            self
          end

          # Aggregate the write errors returned from this result.
          #
          # @example Aggregate the write errors.
          #   result.aggregate_write_errors
          #
          # @return [ Array ] The aggregate write errors.
          #
          # @since 2.0.0
          def aggregate_write_errors
            @replies.each_with_index.reduce([]) do |errors, (reply, i)|
              errors.tap do |e|
                e << { 'errmsg' => reply.documents.first[Operation::ERROR],
                       'index' => indexes[i],
                       'code' => reply.documents.first[Operation::ERROR_CODE]
                     } if reply_write_errors?(reply)
              end
            end
          end

          private

          def reply_write_errors?(reply)
            reply.documents.first[Operation::ERROR] &&
              reply.documents.first[Operation::ERROR_CODE]
          end
        end
      end
    end
  end
end
