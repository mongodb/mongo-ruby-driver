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

    # Execute a read operation with a retry.
    #
    # @api private
    #
    # @example Execute the read.
    #   read_with_retry do
    #     ...
    #   end
    #
    # @note This only retries read operations on socket errors.
    #
    # @param [ Integer ] attempt The retry attempt count - for internal use.
    # @param [ Proc ] block The block to execute.
    #
    # @return [ Result ] The result of the operation.
    #
    # @since 2.1.0
    def read_with_retry
      attempt = 0
      begin
        attempt += 1
        yield
      rescue Error::SocketError, Error::SocketTimeoutError => e
        raise(e) if attempt > cluster.max_read_retries
        retry_reconnect(e)
        retry
      rescue Error::OperationFailure => e
        if cluster.sharded? && e.retryable?
          if attempt <= cluster.max_read_retries
            # We don't scan the cluster in this case as Mongos always returns
            # ready after a ping no matter what the state behind it is.
            sleep(cluster.read_retry_interval)
            retry
          else
            raise e
          end
        else
          raise e
        end
      end
    end

    # Execute a read operation with a single retry.
    #
    # @api private
    #
    # @example Execute the read.
    #   read_with_one_retry do
    #     ...
    #   end
    #
    # @note This only retries read operations on socket errors.
    #
    # @param [ Proc ] block The block to execute.
    #
    # @return [ Result ] The result of the operation.
    #
    # @since 2.2.6
    def read_with_one_retry
      yield
    rescue Error::SocketError, Error::SocketTimeoutError => e
      yield
    end

    # Execute a write operation with a retry.
    #
    # @api private
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
    def write_with_retry
      attempt = 0
      begin
        attempt += 1
        yield
      rescue Error::SocketError => e
        raise(e) if attempt > cluster.max_read_retries
        # During a replicaset master change the primary immediately closes all existing client connections.
        retry_reconnect(e)
        retry
      rescue Error::OperationFailure => e
        raise(e) if attempt > cluster.max_read_retries
        if e.write_retryable?
          retry_reconnect(e)
          retry
        else
          raise(e)
        end
      end
    end

    private

    def retry_reconnect(e)
      # Log a warning so that any application slow down is immediately obvious.
      Logger.logger.warn "Retry due to: #{e.class.name} #{e.message}"
      sleep(cluster.read_retry_interval)
      cluster.scan!
    end
  end
end
