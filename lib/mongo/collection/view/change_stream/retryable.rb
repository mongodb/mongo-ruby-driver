# Copyright (C) 2017 MongoDB, Inc.
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
  class Collection
    class View
      class ChangeStream < Aggregation

        # Behavior around resuming a change stream.
        #
        # @since 2.5.0
        module Retryable

          private

          RETRY_MESSAGES = [
            'not master',
            '(43)' # cursor not found error code
          ].freeze

          def read_with_one_retry
            yield
          rescue => e
            if retryable?(e)
              yield
            else
              raise(e)
            end
          end

          def retryable?(error)
             network_error?(error) || retryable_operation_failure?(error)
          end

          def network_error?(error)
            [ Error::SocketError, Error::SocketTimeoutError].include?(error.class)
          end

          def retryable_operation_failure?(error)
            error.is_a?(Error::OperationFailure) && RETRY_MESSAGES.any? { |m| error.message.include?(m) }
          end
        end
      end
    end
  end
end
