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
      class BulkDelete
  
        # Defines custom behaviour of results when deleting.
        #
        # @since 2.0.0
        class Result < Operation::Result

          attr_reader :indexes

          # The aggregate number of deleted docs reported in the replies.
          #
          # @since 2.0.0
          REMOVED = 'nRemoved'.freeze

          # Gets the number of documents deleted.
          #
          # @example Get the deleted count.
          #   result.n_removed
          #
          # @return [ Integer ] The number of documents deleted.
          #
          # @since 2.0.0
          def n_removed
            return 0 unless acknowledged?
            @replies.reduce(0) do |n, reply|
              n += reply.documents.first[N]
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
            @replies.reduce(nil) do |errors, reply|
              if write_errors = reply.documents.first['writeErrors']
                errors ||= []
                write_errors.each do |write_error|
                  errors << write_error.merge('index' => indexes[write_error['index']])
                end
              end
              errors
            end
          end

          # Aggregate the write concern errors returned from this result.
          #
          # @example Aggregate the write concern errors.
          #   result.aggregate_write_concern_errors
          #
          # @return [ Array ] The aggregate write concern errors.
          #
          # @since 2.0.0
          def aggregate_write_concern_errors
            @replies.reduce(nil) do |errors, reply|
              if write_concern_errors = reply.documents.first['writeConcernErrors']
                errors ||= []
                write_concern_errors.each do |write_concern_error|
                  errors << write_concern_error.merge('index' =>
                                                      indexes[write_concern_error['index']])
                end
              end
              errors
            end
          end
        end

        # Defines custom behaviour of results when deleting.
        # For server versions < 2.5.5 (that don't use write commands).
        #
        # @since 2.0.0
        class LegacyResult < Operation::Result

          attr_reader :indexes

          # Gets the number of documents deleted.
          #
          # @example Get the deleted count.
          #   result.n_removed
          #
          # @return [ Integer ] The number of documents deleted.
          #
          # @since 2.0.0
          def n_removed
            return 0 unless acknowledged?
            @replies.reduce(0) do |n, reply|
              n += reply.documents.first[N]
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
            @replies.each_with_index.reduce(nil) do |errors, (reply, i)|
              if reply_write_errors?(reply)
                errors ||= []
                errors << { 'errmsg' => reply.documents.first[Operation::ERROR],
                            'index' => indexes[i],
                            'code' => reply.documents.first[Operation::ERROR_CODE] }
              end
              errors
            end
          end

          # Aggregate the write concern errors returned from this result.
          #
          # @example Aggregate the write concern errors.
          #   result.aggregate_write_concern_errors
          #
          # @return [ Array ] The aggregate write concern errors.
          #
          # @since 2.0.0
          def aggregate_write_concern_errors
            @replies.each_with_index.reduce(nil) do |errors, (reply, i)|
              # @todo: only raise if error is timeout
              if error = reply_write_errors?(reply)
                errors ||= []
                note = reply.documents.first['wnote'] || reply.documents.first['jnote']
                if note
                  code = reply.documents.first['code'] || Operation::BAD_VALUE
                  error_string = "#{code}: #{note}"
                else
                  code = reply.documents.first['code'] || Operation::UNKNOWN_ERROR
                  error_string = "#{code}: #{error}"
                end
                errors << { 'errmsg' => error_string,
                            'index' => indexes[i],
                            'code' => code }
              end
              errors
            end
          end

          private

          def reply_write_errors?(reply)
            reply.documents.first[Operation::ERROR] ||
              reply.documents.first[Operation::ERROR_MSG]
          end
        end
      end
    end
  end
end
