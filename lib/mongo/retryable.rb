# Copyright (C) 2015 MongoDB, Inc.
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

  # Defines basic behaviour around retrying operations.
  #
  # @since 2.1.0
  module Retryable

    # The not master error message.
    #
    # @since 2.1.0
    NOT_MASTER = 'not master'.freeze

    # Execute a read operation with a retry.
    #
    # @example Execute the read.
    #   read_with_retry do
    #     ...
    #   end
    #
    # @note This only retries read operations on socket errors.
    #
    # @param [ Proc ] block The block to execute.
    #
    # @return [ Result ] The result of the operation.
    #
    # @since 2.1.0
    def read_with_retry(&block)
      begin
        block.call
      rescue Error::SocketError, Error::SocketTimeoutError
        retry_operation(&block)
      end
    end

    # Execute a write operation with a retry.
    #
    # @example Execute the write.
    #   write_with_retry do
    #     ...
    #   end
    #
    # @note This only retries operations on not master failures, since it is
    #   the only case we can be sure a partial write did not already occur.
    #
    # @param [ Proc ] block The block to execute.
    #
    # @return [ Result ] The result of the operation.
    #
    # @since 2.1.0
    def write_with_retry(&block)
      begin
        block.call
      rescue Error::OperationFailure => e
        if e.message.include?(NOT_MASTER)
          retry_operation(&block)
        else
          raise e
        end
      end
    end

    private

    def retry_operation(&block)
      cluster.scan!
      block.call
    end
  end
end
