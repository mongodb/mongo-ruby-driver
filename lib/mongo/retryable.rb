# Copyright (C) 2015-2019 MongoDB, Inc.
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

  # Defines basic behavior around retrying operations.
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
        if attempt > cluster.max_read_retries || (session && session.in_transaction?)
          raise
        end
        log_retry(e)
        cluster.scan!(false)
        retry
      rescue Error::OperationFailure => e
        if cluster.sharded? && e.retryable? && !(session && session.in_transaction?)
          if attempt > cluster.max_read_retries
            raise
          end
          log_retry(e)
          sleep(cluster.read_retry_interval)
          retry
        else
          raise
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
    # @param [ Hash ] options Options.
    # @param [ Proc ] block The block to execute.
    #
    # @option options [ String ] :retry_message Message to log when retrying.
    #
    # @return [ Result ] The result of the operation.
    #
    # @since 2.2.6
    def read_with_one_retry(options = nil)
      yield
    rescue Error::SocketError, Error::SocketTimeoutError => e
      retry_message = options && options[:retry_message]
      log_retry(e, message: retry_message)
      yield
    end

    # Implements write retrying functionality by yielding to the passed
    # block one or more times.
    #
    # If the session is provided (hence, the deployment supports sessions),
    # and modern retry writes are enabled on the client, the modern retry
    # logic is invoked. Otherwise the legacy retry logic is invoked.
    #
    # If ending_transaction parameter is true, indicating that a transaction
    # is being committed or aborted, the operation is executed exactly once.
    # Note that, since transactions require sessions, this method will raise
    # ArgumentError if ending_transaction is true and session is nil.
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
    # @param [ nil | Session ] session Optional session to use with the operation.
    # @param [ nil | Hash | WriteConcern::Base ] write_concern The write concern.
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
      if ending_transaction && !session
        raise ArgumentError, 'Cannot end a transaction without a session'
      end

      unless ending_transaction || retry_write_allowed?(session, write_concern)
        return legacy_write_with_retry(nil, session, &block)
      end

      # If we are here, session is not nil. A session being nil would have
      # failed retry_write_allowed? check.

      server = cluster.next_primary

      unless ending_transaction || server.retry_writes?
        return legacy_write_with_retry(server, session, &block)
      end

      begin
        txn_num = session.in_transaction? ? session.txn_num : session.next_txn_num
        yield(server, txn_num, false)
      rescue Error::SocketError, Error::SocketTimeoutError => e
        if session.in_transaction? && !ending_transaction
          raise
        end
        retry_write(e, txn_num, &block)
      rescue Error::OperationFailure => e
        if (session.in_transaction? && !ending_transaction) || !e.write_retryable?
          raise
        end
        retry_write(e, txn_num, &block)
      end
    end

    private

    def retry_write_allowed?(session, write_concern)
      unless session && session.retry_writes?
        return false
      end

      if write_concern.nil?
        true
      else
        unless write_concern.is_a?(WriteConcern::Base)
          write_concern = WriteConcern.get(write_concern)
        end
        write_concern.acknowledged?
      end
    end

    def retry_write(original_error, txn_num, &block)
      # We do not request a scan of the cluster here, because error handling
      # for the error which triggered the retry should have updated the
      # server description and/or topology as necessary (specifically,
      # a socket error or a not master error should have marked the respective
      # server unknown). Here we just need to wait for server selection.
      server = cluster.next_primary
      raise original_error unless (server.retry_writes? && txn_num)
      log_retry(original_error)
      yield(server, txn_num, true)
    rescue Error::SocketError, Error::SocketTimeoutError => e
      raise e
    rescue Error::OperationFailure => e
      raise original_error unless e.write_retryable?
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
        if attempt > Cluster::MAX_WRITE_RETRIES
          raise
        end
        if e.write_retryable? && !(session && session.in_transaction?)
          log_retry(e)
          cluster.scan!(false)
          retry
        else
          raise
        end
      end
    end

    # Log a warning so that any application slow down is immediately obvious.
    def log_retry(e, options = nil)
      message = if options && options[:message]
        options[:message]
      else
        "Retry"
      end
      Logger.logger.warn "#{message} due to: #{e.class.name} #{e.message}"
    end
  end
end
