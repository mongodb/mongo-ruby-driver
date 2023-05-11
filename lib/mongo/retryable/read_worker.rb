# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2015-2023 MongoDB Inc.
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

require 'mongo/retryable/base_worker'

module Mongo
  module Retryable

    # Implements the logic around retrying read operations.
    #
    # @api private
    #
    # @since 2.19.0
    class ReadWorker < BaseWorker
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
      # @param [ Mongo::Session | nil ] session The session that the operation
      #   is being run on.
      # @param [ Mongo::ServerSelector::Selectable | nil ] server_selector
      #   Server selector for the operation.
      # @param [ Proc ] block The block to execute.
      #
      # @return [ Result ] The result of the operation.
      def read_with_retry(session = nil, server_selector = nil, &block)
        if session.nil? && server_selector.nil?
          deprecated_legacy_read_with_retry(&block)
        elsif session&.retry_reads?
          modern_read_with_retry(session, server_selector, &block)
        elsif client.max_read_retries > 0
          legacy_read_with_retry(session, server_selector, &block)
        else
          read_without_retry(session, server_selector, &block)
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
      # @param [ Hash | nil ] options Options.
      #
      # @option options [ String ] :retry_message Message to log when retrying.
      #
      # @yield Calls the provided block with no arguments
      #
      # @return [ Result ] The result of the operation.
      #
      # @since 2.2.6
      def read_with_one_retry(options = nil)
        yield
      rescue *retryable_exceptions, Error::PoolError => e
        raise e unless e.write_retryable?

        retry_message = options && options[:retry_message]
        log_retry(e, message: retry_message)
        yield
      end

      private

      # Attempts to do a legacy read_with_retry, without either a session or
      # server_selector. This is a deprecated use-case, and a warning will be
      # issued the first time this is invoked.
      #
      # @param [ Proc ] block The block to execute.
      #
      # @return [ Result ] The result of the operation.
      def deprecated_legacy_read_with_retry(&block)
        deprecation_warning :read_with_retry,
          'Legacy read_with_retry invocation - ' \
          'please update the application and/or its dependencies'

        # Since we don't have a session, we cannot use the modern read retries.
        # And we need to select a server but we don't have a server selector.
        # Use PrimaryPreferred which will work as long as there is a data
        # bearing node in the cluster; the block may select a different server
        # which is fine.
        server_selector = ServerSelector.get(mode: :primary_preferred)
        legacy_read_with_retry(nil, server_selector, &block)
      end

      # Attempts to do a "modern" read with retry. Only a single retry will
      # be attempted.
      #
      # @param [ Mongo::Session ] session The session that the operation is
      #   being run on.
      # @param [ Mongo::ServerSelector::Selectable ] server_selector Server
      #   selector for the operation.
      # @param [ Proc ] block The block to execute.
      #
      # @return [ Result ] The result of the operation.
      def modern_read_with_retry(session, server_selector, &block)
        yield select_server(cluster, server_selector, session)
      rescue *retryable_exceptions, Error::OperationFailure, Auth::Unauthorized, Error::PoolError => e
        e.add_notes('modern retry', 'attempt 1')
        raise e if session.in_transaction?
        raise e if !is_retryable_exception?(e) && !e.write_retryable?
        retry_read(e, session, server_selector, &block)
      end
  
      # Attempts to do a "legacy" read with retry. The operation will be
      # attempted multiple times, up to the client's `max_read_retries`
      # setting.
      #
      # @param [ Mongo::Session ] session The session that the operation is
      #   being run on.
      # @param [ Mongo::ServerSelector::Selectable ] server_selector Server
      #   selector for the operation.
      # @param [ Proc ] block The block to execute.
      #
      # @return [ Result ] The result of the operation.
      def legacy_read_with_retry(session, server_selector, &block)
        attempt = attempt ? attempt + 1 : 1
        yield select_server(cluster, server_selector, session)
      rescue *retryable_exceptions, Error::OperationFailure, Error::PoolError => e
        e.add_notes('legacy retry', "attempt #{attempt}")
        
        if is_retryable_exception?(e)
          raise e if attempt > client.max_read_retries || session&.in_transaction?
        elsif e.retryable? && !session&.in_transaction?
          raise e if attempt > client.max_read_retries
        else
          raise e
        end
        
        log_retry(e, message: 'Legacy read retry')
        sleep(client.read_retry_interval) unless is_retryable_exception?(e)
        retry
      end

      # Attempts to do a read *without* a retry; for example, when retries have
      # been explicitly disabled.
      #
      # @param [ Mongo::Session ] session The session that the operation is
      #   being run on.
      # @param [ Mongo::ServerSelector::Selectable ] server_selector Server
      #   selector for the operation.
      # @param [ Proc ] block The block to execute.
      #
      # @return [ Result ] The result of the operation.
      def read_without_retry(session, server_selector, &block)
        server = select_server(cluster, server_selector, session)

        begin
          yield server
        rescue *retryable_exceptions, Error::PoolError, Error::OperationFailure => e
          e.add_note('retries disabled')
          raise e
        end
      end

      # The retry logic of the "modern" read_with_retry implementation.
      #
      # @param [ Exception ] original_error The original error that triggered
      #   the retry.
      # @param [ Mongo::Session ] session The session that the operation is
      #   being run on.
      # @param [ Mongo::ServerSelector::Selectable ] server_selector Server
      #   selector for the operation.
      # @param [ Proc ] block The block to execute.
      # 
      # @return [ Result ] The result of the operation.
      def retry_read(original_error, session, server_selector, &block)
        begin
          server = select_server(cluster, server_selector, session)
        rescue Error, Error::AuthError => e
          original_error.add_note("later retry failed: #{e.class}: #{e}")
          raise original_error
        end
  
        log_retry(original_error, message: 'Read retry')
  
        begin
          yield server, true
        rescue *retryable_exceptions => e
          e.add_notes('modern retry', 'attempt 2')
          raise e
        rescue Error::OperationFailure, Error::PoolError => e
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
      end
  
    end

  end
end
