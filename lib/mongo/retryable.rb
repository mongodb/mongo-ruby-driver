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

    # Execute a read operation returning a cursor with retrying.
    #
    # This method performs server selection for the specified server selector
    # and yields to the provided block, which should execute the initial
    # query operation and return its result. The block will be passed the
    # server selected for the operation. If the block raises an exception,
    # and this exception corresponds to a read retryable error, and read
    # retries are enabled for the client, this method will perform server
    # selection again and yield to the block again (with potentially a
    # different server). If the block returns successfully, the result
    # of the block (which should be a Mongo::Operation::Result) is used to
    # construct a Mongo::Cursor object for the result set. The cursor
    # is then returned.
    #
    # If modern retry reads are on (which is the default), the initial read
    # operation will be retried once. If legacy retry reads are on, the
    # initial read operation will be retried zero or more times depending
    # on the :max_read_retries client setting, the default for which is 1.
    # To disable read retries, turn off modern read retries by setting
    # retry_reads: false and set :max_read_retries to 0 on the client.
    #
    # @api private
    #
    # @example Execute a read returning a cursor.
    #   cursor = read_with_retry_cursor(session, server_selector, view) do |server|
    #     # return a Mongo::Operation::Result
    #     ...
    #   end
    #
    # @param [ Mongo::Session ] session The session that the operation is being
    #   run on.
    # @param [ Mongo::ServerSelector::Selectable ] server_selector Server
    #   selector for the operation.
    # @param [ CollectionView ] view The +CollectionView+ defining the query.
    # @param [ Proc ] block The block to execute.
    #
    # @return [ Cursor ] The cursor for the result set.
    def read_with_retry_cursor(session, server_selector, view, &block)
      read_with_retry(session, server_selector) do |server|
        result = yield server
        Cursor.new(view, result, server, session: session)
      end
    end

    # Execute a read operation with retrying.
    #
    # This method performs server selection for the specified server selector
    # and yields to the provided block, which should execute the initial
    # query operation and return its result. The block will be passed the
    # server selected for the operation. If the block raises an exception,
    # and this exception corresponds to a read retryable error, and read
    # retries are enabled for the client, this method will perform server
    # selection again and yield to the block again (with potentially a
    # different server). If the block returns successfully, the result
    # of the block is returned.
    #
    # If modern retry reads are on (which is the default), the initial read
    # operation will be retried once. If legacy retry reads are on, the
    # initial read operation will be retried zero or more times depending
    # on the :max_read_retries client setting, the default for which is 1.
    # To disable read retries, turn off modern read retries by setting
    # retry_reads: false and set :max_read_retries to 0 on the client.
    #
    # @api private
    #
    # @example Execute the read.
    #   read_with_retry(session, server_selector) do |server|
    #     ...
    #   end
    #
    # @param [ Mongo::Session ] session The session that the operation is being
    #   run on.
    # @param [ Mongo::ServerSelector::Selectable ] server_selector Server
    #   selector for the operation.
    # @param [ Proc ] block The block to execute.
    #
    # @return [ Result ] The result of the operation.
    def read_with_retry(session = nil, server_selector = nil, &block)
      if session.nil? && server_selector.nil?
        # Older versions of Mongoid call read_with_retry without arguments.
        # This is already not correct in a MongoDB 3.6+ environment with
        # sessions. For compatibility we emulate the legacy driver behavior
        # here but upgrading Mongoid is strongly recommended.
        unless $_mongo_read_with_retry_warned
          $_mongo_read_with_retry_warned = true
          Logger.logger.warn("Legacy read_with_retry invocation - please update the application and/or its dependencies")
        end
        # Since we don't have a session, we cannot use the modern read retries.
        # And we need to select a server but we don't have a server selector.
        # Use PrimaryPreferred which will work as long as there is a data
        # bearing node in the cluster; the block may select a different server
        # which is fine.
        server_selector = ServerSelector.get(mode: :primary_preferred)
        legacy_read_with_retry(nil, server_selector, &block)
      elsif session && session.retry_reads?
        modern_read_with_retry(session, server_selector, &block)
      elsif client.max_read_retries > 0
        legacy_read_with_retry(session, server_selector, &block)
      else
        server = select_server(cluster, server_selector, session)
        yield server
      end
    end

    # Execute a read operation with a single retry on network errors.
    #
    # This method is used by the driver for some of the internal housekeeping
    # operations. Application-requested reads should use read_with_retry
    # rather than this method.
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

      server = select_server(cluster, ServerSelector.primary, session)

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
        retry_write(e, session, txn_num, &block)
      rescue Error::OperationFailure => e
        if (session.in_transaction? && !ending_transaction) || !e.write_retryable?
          raise
        end
        retry_write(e, session, txn_num, &block)
      end
    end

    # Implements legacy write retrying functionality by yielding to the passed
    # block one or more times.
    #
    # This method is used for operations which are not supported by modern
    # retryable writes, such as delete_many and update_many.
    #
    # @param [ Server ] server The server which should be used for the
    #   operation. If not provided, the current primary will be retrieved from
    #   the cluster.
    # @param [ nil | Session ] session Optional session to use with the operation.
    #
    # @api private
    def legacy_write_with_retry(server = nil, session = nil)
      # This is the pre-session retry logic, and is not subject to
      # current retryable write specifications.
      # In particular it does not retry on SocketError and SocketTimeoutError.
      attempt = 0
      begin
        attempt += 1
        server ||= select_server(cluster, ServerSelector.primary, session)
        yield server
      rescue Error::OperationFailure => e
        server = nil
        if attempt > client.max_write_retries
          raise
        end
        if e.write_retryable? && !(session && session.in_transaction?)
          log_retry(e, message: 'Legacy write retry')
          cluster.scan!(false)
          retry
        else
          raise
        end
      end
    end

    private

    def modern_read_with_retry(session, server_selector, &block)
      attempt = 0
      server = select_server(cluster, server_selector, session)
      begin
        yield server
      rescue Error::SocketError, Error::SocketTimeoutError => e
        if session.in_transaction?
          raise
        end
        retry_read(e, server_selector, session, &block)
      rescue Error::OperationFailure => e
        if session.in_transaction? || !e.write_retryable?
          raise
        end
        retry_read(e, server_selector, session, &block)
      end
    end

    def legacy_read_with_retry(session, server_selector)
      attempt = 0
      server = select_server(cluster, server_selector, session)
      begin
        attempt += 1
        yield server
      rescue Error::SocketError, Error::SocketTimeoutError => e
        if attempt > client.max_read_retries || (session && session.in_transaction?)
          raise
        end
        log_retry(e, message: 'Legacy read retry')
        server = select_server(cluster, server_selector, session)
        retry
      rescue Error::OperationFailure => e
        if cluster.sharded? && e.retryable? && !(session && session.in_transaction?)
          if attempt > client.max_read_retries
            raise
          end
          log_retry(e, message: 'Legacy read retry')
          sleep(client.read_retry_interval)
          server = select_server(cluster, server_selector, session)
          retry
        else
          raise
        end
      end
    end

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

    def retry_read(original_error, server_selector, session, &block)
      begin
        server = select_server(cluster, server_selector, session)
      rescue
        raise original_error
      end

      log_retry(original_error, message: 'Read retry')

      begin
        yield server, true
      rescue Error::SocketError, Error::SocketTimeoutError => e
        raise e
      rescue Error::OperationFailure => e
        raise original_error unless e.write_retryable?
        raise e
      rescue
        raise original_error
      end
    end

    def retry_write(original_error, session, txn_num, &block)
      # We do not request a scan of the cluster here, because error handling
      # for the error which triggered the retry should have updated the
      # server description and/or topology as necessary (specifically,
      # a socket error or a not master error should have marked the respective
      # server unknown). Here we just need to wait for server selection.
      server = select_server(cluster, ServerSelector.primary, session)
      raise original_error unless (server.retry_writes? && txn_num)
      log_retry(original_error, message: 'Write retry')
      yield(server, txn_num, true)
    rescue Error::SocketError, Error::SocketTimeoutError => e
      raise e
    rescue Error::OperationFailure => e
      raise original_error unless e.write_retryable?
      raise e
    rescue
      raise original_error
    end

    # This is a separate method to make it possible for the test suite to
    # assert that server selection is performed during retry attempts.
    def select_server(cluster, server_selector, session)
      server_selector.select_server(cluster, nil, session)
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
