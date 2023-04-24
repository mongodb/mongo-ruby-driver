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
      def initialize(client: nil, session: nil, connection_global_id: nil, options: nil)
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
        @connection_global_id = connection_global_id
        @options = options
      end

      attr_reader :client
      attr_reader :session
      attr_reader :options

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
    end
  end
end
