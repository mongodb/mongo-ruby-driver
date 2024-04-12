# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2021 MongoDB Inc.
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

    # Context for operations.
    #
    # Holds various objects needed to make decisions about operation execution
    # in a single container, and provides facade methods for the contained
    # objects.
    #
    # The context contains parameters for operations, and as such while an
    # operation is being prepared nothing in the context should change.
    # When the result of the operation is being processed, the data
    # returned by the context may change (for example, because a transaction
    # is aborted), but at that point the operation should no longer read
    # anything from the context. Because context data may change during
    # operation execution, context objects should not be reused for multiple
    # operations.
    #
    # @api private
    class Context
      def initialize(
        client: nil,
        session: nil,
        connection_global_id: nil,
        operation_timeouts: {},
        view: nil,
        options: nil
      )
        if options
          if client
            raise ArgumentError, 'Client and options cannot both be specified'
          end

          if session
            raise ArgumentError, 'Session and options cannot both be specified'
          end
        end

        if connection_global_id && session&.pinned_connection_global_id
          raise ArgumentError, 'Trying to pin context to a connection when the session is already pinned to a connection.'
        end

        @client = client
        @session = session
        @view = view
        @connection_global_id = connection_global_id
        @deadline = calculate_deadline(operation_timeouts, session)
        @operation_timeouts = operation_timeouts
        @timeout_sec = if @deadline then @deadline - Utils.monotonic_time end
        @options = options
      end

      attr_reader :client
      attr_reader :session
      attr_reader :view
      attr_reader :deadline
      attr_reader :timeout_sec
      attr_reader :options
      attr_reader :operation_timeouts

      # Returns a new Operation::Context with the deadline refreshed
      # and relative to the current moment.
      #
      # @return [ Operation::Context ] the refreshed context
      def refresh(connection_global_id: @connection_global_id, timeout_ms: nil, view: nil)
        operation_timeouts = @operation_timeouts
        operation_timeouts = operation_timeouts.merge(operation_timeout_ms: timeout_ms) if timeout_ms

        self.class.new(client: client,
                       session: session,
                       connection_global_id: connection_global_id,
                       operation_timeouts: operation_timeouts,
                       view: view || self.view,
                       options: options)
      end

      def timeout_ms
        @operation_timeouts[:inherited_timeout_ms] ||
          @operation_timeouts[:operation_timeout_ms]
      end

      def connection_global_id
        @connection_global_id || session&.pinned_connection_global_id
      end

      def in_transaction?
        session&.in_transaction? || false
      end

      def starting_transaction?
        session&.starting_transaction? || false
      end

      def committing_transaction?
        in_transaction? && session.committing_transaction?
      end

      def aborting_transaction?
        in_transaction? && session.aborting_transaction?
      end

      def modern_retry_writes?
        client && client.options[:retry_writes]
      end

      def legacy_retry_writes?
        client && !client.options[:retry_writes] && client.max_write_retries > 0
      end

      def any_retry_writes?
        modern_retry_writes? || legacy_retry_writes?
      end

      def server_api
        if client
          client.options[:server_api]
        elsif options
          options[:server_api]
        end
      end

      # Whether the operation is a retry (true) or an initial attempt (false).
      def retry?
        !!@is_retry
      end

      # Returns a new context with the parameters changed as per the
      # provided arguments.
      #
      # @option opts [ true|false ] :is_retry Whether the operation is a retry
      #   or a first attempt.
      def with(**opts)
        dup.tap do |copy|
          opts.each do |k, v|
            copy.instance_variable_set("@#{k}", v)
          end
        end
      end

      def encrypt?
        client&.encrypter&.encrypt? || false
      end

      def decrypt?
        !!client&.encrypter
      end

      def encrypter
        if client&.encrypter
          client.encrypter
        else
          raise Error::InternalDriverError, 'Encrypter should only be accessed when encryption is to be performed'
        end
      end

      # @return [ true | false ] Whether CSOT is enabled for the operation
      def csot?
        !deadline.nil?
      end

      # @return [ true | false ] Returns false if CSOT is not enabled, or if
      #   CSOT is set to 0 (means unlimited), otherwise true.
      def has_timeout?
        ![nil, 0].include?(@deadline)
      end

      # @return [ Float | nil ] Returns the remaining seconds of the timeout
      #   set for the operation; if no timeout is set, or the timeout is 0
      #   (means unlimited) returns nil.
      def remaining_timeout_sec
        return nil unless has_timeout?

        deadline - Utils.monotonic_time
      end

      # @return [ Integer | nil ] Returns the remaining milliseconds of the timeout
      #   set for the operation; if no timeout is set, or the timeout is 0
      #   (means unlimited) returns nil.
      def remaining_timeout_ms
        seconds = remaining_timeout_sec
        return nil if seconds.nil?

        (seconds * 1_000).to_i
      end

      def inspect
        "#<#{self.class} connection_global_id=#{connection_global_id.inspect} deadline=#{deadline.inspect} options=#{options.inspect} operation_timeouts=#{operation_timeouts.inspect}>"
      end

      # @return [ true | false ] Whether the timeout for the operation expired.
      #   If no timeout set, this method returns false.
      def timeout_expired?
        if has_timeout?
          Utils.monotonic_time >= deadline
        else
          false
        end
      end

      # Check whether the operation timeout expired, and raises an appropriate
      # error if yes.
      #
      # @raise [ Error::TimeoutError ]
      def check_timeout!
        if timeout_expired?
          raise Error::TimeoutError, "Operation took more than #{timeout_sec} seconds"
        end
      end

      private

      def calculate_deadline(opts = {}, session = nil)
        if opts[:operation_timeout_ms] && session&.with_transaction_deadline
          raise ArgumentError, 'Cannot override timeout_ms inside with_transaction block'
        end

        if session&.with_transaction_deadline
          session&.with_transaction_deadline
        elsif operation_timeout_ms = opts[:operation_timeout_ms]
          if operation_timeout_ms > 0
            Utils.monotonic_time + (operation_timeout_ms / 1_000.0)
          elsif operation_timeout_ms == 0
            0
          elsif operation_timeout_ms < 0
            raise ArgumentError, /must be a non-negative integer/
          end
        elsif inherited_timeout_ms = opts[:inherited_timeout_ms]
          if inherited_timeout_ms > 0
            Utils.monotonic_time + (inherited_timeout_ms / 1_000.0)
          elsif inherited_timeout_ms == 0
            0
          end
        end
      end
    end
  end
end
