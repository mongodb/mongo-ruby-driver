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

        # This module contains common functionality for merging results from
        # writes during a bulk operation. Used for server versions < 2.6.
        #
        # @since 2.0.0
        module LegacyMergable

          # Aggregate the write errors returned from this result.
          #
          # @example Aggregate the write errors.
          #   result.aggregate_write_errors(0)
          #
          # @param [ Integer ] count The number of documents already executed.
          #
          # @return [ Array ] The aggregate write errors.
          #
          # @since 2.0.0
          def aggregate_write_errors(count)
            @replies.each_with_index.reduce(nil) do |errors, (reply, i)|
              if reply_write_errors?(reply)
                errors ||= []
                errors << { 'errmsg' => reply.documents.first[Error::ERROR],
                            'index' => count + i,
                            'code' => reply.documents.first[Error::CODE] }
              end
              errors
            end if @replies
          end

          # Aggregate the write concern errors returned from this result.
          #
          # @example Aggregate the write concern errors.
          #   result.aggregate_write_concern_errors(4)
          #
          # @param [ Integer ] count The number of documents already executed.
          #
          # @return [ Array ] The aggregate write concern errors.
          #
          # @since 2.0.0
          def aggregate_write_concern_errors(count)
            @replies.each_with_index.reduce(nil) do |errors, (reply, i)|
              if error = reply_write_errors?(reply)
                errors ||= []
                if note = reply.documents.first['wnote'] || reply.documents.first['jnote']
                  code = reply.documents.first['code'] || Error::BAD_VALUE
                  error_string = "#{code}: #{note}"
                elsif error == 'timeout'
                  code = reply.documents.first['code'] || Error::UNKNOWN_ERROR
                  error_string = "#{code}: #{error}"
                end
                errors << { 'errmsg' => error_string,
                            'index' => count + i,
                            'code' => code } if error_string
              end
              errors
            end if @replies
          end

          private

          def reply_write_errors?(reply)
            reply.documents.first[Error::ERROR] ||
              reply.documents.first[Error::ERRMSG]
          end
        end
      end
    end
  end
end
