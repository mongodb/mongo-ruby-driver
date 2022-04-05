# frozen_string_literal: true
# encoding: utf-8

# Copyright (C) 2015-2020 MongoDB Inc.
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

        # RUBY-2367: This will be updated to allow the query cache to
        # cache cursors with multi-batch results.
        if QueryCache.enabled? && !view.collection.system_collection?
          CachingCursor.new(view, result, server, session: session)
        else
          Cursor.new(view, result, server, session: session)
        end
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
        begin
          yield server
        rescue Error::SocketError, Error::SocketTimeoutError, Error::OperationFailure => e
          e.add_note('retries disabled')
          raise e
        end
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
    # @yield Calls the provided block with no arguments
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
    # @param [ nil | Hash | WriteConcern::Base ] write_concern The write concern.
    # @param [ true | false ] ending_transaction True if the write operation is
    #   abortTransaction or commitTransaction, false otherwise.
    # @param [ Context ] context The context for the operation.
    # @param [ Proc ] block The block to execute.
    #
    # @yieldparam [ Connection ] connection The connection through which the
    #   write should be sent.
    # @yieldparam [ Integer ] txn_num Transaction number (NOT the ACID kind).
    # @yieldparam [ Operation::Context ] context The operation context.
    #
    # @return [ Result ] The result of the operation.
    #
    # @since 2.1.0
    def write_with_retry(write_concern, ending_transaction: false, context:, &block)
      session = context.session

      if ending_transaction && !session
        raise ArgumentError, 'Cannot end a transaction without a session'
      end

      unless ending_transaction || retry_write_allowed?(session, write_concern)
        return legacy_write_with_retry(nil, context: context, &block)
      end

      # If we are here, session is not nil. A session being nil would have
      # failed retry_write_allowed? check.

      server = select_server(cluster, ServerSelector.primary, session)

      unless ending_transaction || server.retry_writes?
        return legacy_write_with_retry(server, context: context, &block)
      end

      txn_num = nil

      begin
        connection_succeeded = false
        server.with_connection(connection_global_id: context.connection_global_id) do |connection|
          connection_succeeded = true

          session.materialize_if_needed
          txn_num = if session.in_transaction?
            session.txn_num
          else
            session.next_txn_num
          end

          # The context needs to be duplicated here because we will be using
          # it later for the retry as well.
          yield(connection, txn_num, context.dup)
        end
      rescue Error::SocketError, Error::SocketTimeoutError, Auth::Unauthorized => e
        e.add_note('modern retry')
        e.add_note("attempt 1")
        if !e.label?('RetryableWriteError')
          # If we get an auth error, it was raised when connecting the connection
          # and therefore we didn't have the connection yet to add labels.
          # Therefore, check if it is retryable, and if it is, add the label
          # and retry it. We also want to retry this if there was a Socket error
          # when trying to create the connection.
          if !connection_succeeded && !session.in_transaction? && e.write_retryable?
            e.add_label('RetryableWriteError')
          else
            raise e
          end
        end

        # Context#with creates a new context, which is not necessary here
        # but the API is less prone to misuse this way.
        retry_write(e, txn_num, context: context.with(is_retry: true), &block)
      rescue Error::OperationFailure => e
        e.add_note('modern retry')
        e.add_note("attempt 1")
        if e.unsupported_retryable_write?
          raise_unsupported_error(e)
        elsif !e.label?('RetryableWriteError')
          raise e
        end

        # Context#with creates a new context, which is not necessary here
        # but the API is less prone to misuse this way.
        retry_write(e, txn_num, context: context.with(is_retry: true), &block)
      end
    end

    # Retryable writes wrapper for operations not supporting modern retryable
    # writes.
    #
    # If the driver is configured to use modern retryable writes, this method
    # yields to the passed block exactly once, thus not retrying any writes.
    #
    # If the driver is configured to use legacy retryable writes, this method
    # delegates to legacy_write_with_retry which performs write retries using
    # legacy logic.
    #
    # @param [ nil | Hash | WriteConcern::Base ] write_concern The write concern.
    # @param [ Context ] context The context for the operation.
    #
    # @yieldparam [ Connection ] connection The connection through which the
    #   write should be sent.
    # @yieldparam [ nil ] txn_num nil as transaction number.
    # @yieldparam [ Operation::Context ] context The operation context.
    #
    # @api private
    def nro_write_with_retry(write_concern, context:, &block)
      session = context.session

      server = select_server(cluster, ServerSelector.primary, session)
      if session && session.client.options[:retry_writes]
        begin
          server.with_connection(connection_global_id: context.connection_global_id) do |connection|
            yield connection, nil, context
          end
        rescue Error::SocketError, Error::SocketTimeoutError, Error::OperationFailure => e
          e.add_note('retries disabled')
          raise e
        end
      else
        legacy_write_with_retry(server, context: context, &block)
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
    # @param [ Context ] context The context for the operation.
    #
    # @yieldparam [ Connection ] connection The connection through which the
    #    write should be sent.
    # @yieldparam [ nil ] txn_num nil as transaction number.
    # @yieldparam [ Operation::Context ] context The operation context.
    #
    # @api private
    def legacy_write_with_retry(server = nil, context:)
      session = context.session

      # This is the pre-session retry logic, and is not subject to
      # current retryable write specifications.
      # In particular it does not retry on SocketError and SocketTimeoutError.
      attempt = 0
      begin
        attempt += 1
        server ||= select_server(cluster, ServerSelector.primary, session)
        server.with_connection(connection_global_id: context.connection_global_id) do |connection|
          # Legacy retries do not use txn_num
          yield connection, nil, context.dup
        end
      rescue Error::OperationFailure => e
        e.add_note('legacy retry')
        e.add_note("attempt #{attempt}")
        server = nil
        if attempt > client.max_write_retries
          raise e
        end
        if e.label?('RetryableWriteError')
          log_retry(e, message: 'Legacy write retry')
          cluster.scan!(false)
          retry
        else
          raise e
        end
      end
    end

    private

    def modern_read_with_retry(session, server_selector, &block)
      server = select_server(cluster, server_selector, session)
      begin
        yield server
      rescue Error::SocketError, Error::SocketTimeoutError => e
        e.add_note('modern retry')
        e.add_note("attempt 1")
        if session.in_transaction?
          raise e
        end
        retry_read(e, server_selector, session, &block)
      rescue Error::OperationFailure, Auth::Unauthorized => e
        e.add_note('modern retry')
        e.add_note("attempt 1")
        if session.in_transaction? || !e.write_retryable?
          raise e
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
        e.add_note('legacy retry')
        e.add_note("attempt #{attempt}")
        if attempt > client.max_read_retries || (session && session.in_transaction?)
          raise e
        end
        log_retry(e, message: 'Legacy read retry')
        server = select_server(cluster, server_selector, session)
        retry
      rescue Error::OperationFailure => e
        e.add_note('legacy retry')
        e.add_note("attempt #{attempt}")
        if e.retryable? && !(session && session.in_transaction?)
          if attempt > client.max_read_retries
            raise e
          end
          log_retry(e, message: 'Legacy read retry')
          sleep(client.read_retry_interval)
          server = select_server(cluster, server_selector, session)
          retry
        else
          raise e
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
      rescue Error, Error::AuthError => e
        original_error.add_note("later retry failed: #{e.class}: #{e}")

        # See the corresponding note below in retry_write.
        raise Error::RaiseOriginalError
      end

      log_retry(original_error, message: 'Read retry')

      begin
        yield server, true
      rescue Error::SocketError, Error::SocketTimeoutError => e
        e.add_note('modern retry')
        e.add_note("attempt 2")
        raise e
      rescue Error::OperationFailure => e
        e.add_note('modern retry')
        unless e.write_retryable?
          original_error.add_note("later retry failed: #{e.class}: #{e}")
          raise original_error
        end
        e.add_note("attempt 2")
        raise e
      rescue Error, Error::AuthError => e
        e.add_note('modern retry')
        original_error.add_note("later retry failed: #{e.class}: #{e}")
        raise original_error
      end
    rescue Error::RaiseOriginalError
      raise original_error
    end

    def retry_write(original_error, txn_num, context:, &block)
      session = context.session

      # We do not request a scan of the cluster here, because error handling
      # for the error which triggered the retry should have updated the
      # server description and/or topology as necessary (specifically,
      # a socket error or a not master error should have marked the respective
      # server unknown). Here we just need to wait for server selection.
      server = select_server(cluster, ServerSelector.primary, session)
      unless server.retry_writes?
        # Do not need to add "modern retry" here, it should already be on
        # the first exception.
        original_error.add_note('did not retry because server selected for retry does not supoprt retryable writes')

        # When we want to raise the original error, we must not run the
        # rescue blocks below that add diagnostics because the diagnostics
        # added would either be rendundant (e.g. modern retry note) or wrong
        # (e.g. "attempt 2", we are raising the exception produced in the
        # first attempt and haven't attempted the second time). Use the
        # special marker class to bypass the ordinarily applicable rescues.
        raise Error::RaiseOriginalError
      end
      log_retry(original_error, message: 'Write retry')
      server.with_connection(connection_global_id: context.connection_global_id) do |connection|
        yield(connection, txn_num, context)
      end
    rescue Error::SocketError, Error::SocketTimeoutError => e
      e.add_note('modern retry')
      e.add_note('attempt 2')
      raise e
    rescue Error::OperationFailure => e
      e.add_note('modern retry')
      if e.label?('RetryableWriteError')
        e.add_note('attempt 2')
        raise e
      else
        original_error.add_note("later retry failed: #{e.class}: #{e}")
        raise original_error
      end
    rescue Error, Error::AuthError => e
      # Do not need to add "modern retry" here, it should already be on
      # the first exception.
      original_error.add_note("later retry failed: #{e.class}: #{e}")
      raise original_error
    rescue Error::RaiseOriginalError
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
      Logger.logger.warn "#{message} due to: #{e.class.name}: #{e.message}"
    end

    # Retry writes on MMAPv1 should raise an actionable error; append actionable
    # information to the error message and preserve the backtrace.
    def raise_unsupported_error(e)
      new_error = Error::OperationFailure.new("#{e.class}: #{e} "\
        "This MongoDB deployment does not support retryable writes. Please add "\
        "retryWrites=false to your connection string or use the retry_writes: false Ruby client option")
      new_error.set_backtrace(e.backtrace)
      raise new_error
    end
  end
end
