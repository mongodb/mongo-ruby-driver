# Copyright (C) 2015-2018 MongoDB, Inc.
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
    # @param [ Mongo::Session ] session The session that the operation is being run on.
    # @param [ Proc ] block The block to execute.
    #
    # @return [ Result ] The result of the operation.
    #
    # @since 2.1.0
    def read_with_retry(session = nil)
      attempt = 0
      begin
        attempt += 1
        yield
      rescue Error::SocketError, Error::SocketTimeoutError => e
        raise(e) if attempt > cluster.max_read_retries || (session && session.in_transaction?)
        log_retry(e)
        cluster.scan!(false)
        retry
      rescue Error::OperationFailure => e
        if cluster.sharded? && e.retryable? && !(session && session.in_transaction?)
          raise(e) if attempt > cluster.max_read_retries
          log_retry(e)
          sleep(cluster.read_retry_interval)
          retry
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
    rescue Error::SocketError, Error::SocketTimeoutError
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
    # @param [ true | false ] ending_transaction True if the write operation is abortTransaction or
    #   commitTransaction, false otherwise.
    # @param [ Proc ] block The block to execute.
    #
    # @yieldparam [ Server ] server The server to which the write should be sent.
    # @yieldparam [ Integer ] txn_num Transaction number (NOT the ACID kind).
    #
    # @return [ Result ] The result of the operation.
    #
    # @since 2.1.0
    def write_with_retry(session, write_concern, ending_transaction = false, &block)
      unless retry_write_allowed?(session, write_concern) || ending_transaction
        return legacy_write_with_retry(nil, session, &block)
      end

      server = cluster.next_primary

      unless server.retry_writes? || ending_transaction
        return legacy_write_with_retry(server, session, &block)
      end

      begin
        txn_num = session.in_transaction? ? session.txn_num : session.next_txn_num
        yield(server, txn_num)
      rescue Error::SocketError, Error::SocketTimeoutError => e
        raise e if session.in_transaction? && !ending_transaction
        retry_write(e, txn_num, &block)
      rescue Error::OperationFailure => e
        raise e if (session.in_transaction? && !ending_transaction) || !e.write_retryable?
        retry_write(e, txn_num, &block)
      end
    end

    private

    def retry_write_allowed?(session, write_concern)
      session && session.retry_writes? &&
          (write_concern.nil? || write_concern.acknowledged?) or false
    end

    def retry_write(original_error, txn_num, &block)
      cluster.scan!(false)
      server = cluster.next_primary
      raise original_error unless (server.retry_writes? && txn_num)
      log_retry(original_error)
      yield(server, txn_num)
    rescue Error::SocketError, Error::SocketTimeoutError => e
      cluster.scan!(false)
      raise e
    rescue Error::OperationFailure => e
      raise original_error unless e.write_retryable?
      cluster.scan!(false)
      raise e
    rescue
      raise original_error
    end

    def legacy_write_with_retry(server = nil, session = nil)
      # This is the pre-session retry logic, and is not subject to
      # current retryable write specifications.
      # In particular it does not retry on SocketError and SocketTimeoutError.
      attempt = 0
      begin
        attempt += 1
        yield(server || cluster.next_primary)
      rescue Error::OperationFailure => e
        server = nil
        raise(e) if attempt > Cluster::MAX_WRITE_RETRIES
        if e.write_retryable? && !(session && session.in_transaction?)
          log_retry(e)
          cluster.scan!(false)
          retry
        else
          raise(e)
        end
      end
    end

    # Log a warning so that any application slow down is immediately obvious.
    def log_retry(e)
      Logger.logger.warn "Retry due to: #{e.class.name} #{e.message}"
    end
  end
end
