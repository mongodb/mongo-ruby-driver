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
      class BulkUpdate

        # Defines custom behaviour of results when updating.
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

          # Aggregate the write errors returned from this result.
          #
          # @example Aggregate the write errors.
          #   result.aggregate_write_errors
          #
          # @return [ Array ] The aggregate write errors.
          #
          # @since 2.0.0
          def aggregate_write_errors(indexes)
            @replies.reduce(nil) do |errors, reply|
              if reply.documents.first['writeErrors']
                write_errors = reply.documents.first['writeErrors'].collect do |we|
                  we.merge!('index' => indexes[we['index']])
                end
                (errors || []) << write_errors if write_errors
              end
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
              if write_concern_errors = reply.documents.first['writeConcernError']
                errors ||= []
                write_concern_errors.each do |write_concern_error|
                  errors << write_concern_error
                end
              elsif reply.documents.first['errmsg']
                errors ||= []
                errors << { 'errmsg' => reply.documents.first['errmsg'],
                            'code' => reply.documents.first['code'] }
              end
              errors
            end
          end

          private

          def upsert?(reply)
            reply.documents.first[UPSERTED]
          end
        end

        # Defines custom behaviour of results when updating.
        # For server versions < 2.5.5 (that don't use write commands).
        #
        # @since 2.0.0
        class LegacyResult < Operation::Result

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
                errors << { 'errmsg' => reply.documents.first[Error::ERROR],
                            'code' => reply.documents.first[Error::CODE] }
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
                  code = reply.documents.first['code'] || "bad value constant"
                  error_string = "#{code}: #{note}"
                else
                  code = reply.documents.first['code'] || "unknown error constant"
                  error_string = "#{code}: #{error}"
                end
                errors << { 'errmsg' => error_string,
                            'code' => code }
              end
              errors
            end
          end

          private

          def reply_write_errors?(reply)
            reply.documents.first[Error::ERROR] ||
              reply.documents.first[Error::ERRMSG]
          end

          def upsert?(reply)
            !reply.documents.first[UPDATED_EXISTING]
          end
        end
      end
    end
  end
end
