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

module Mongo
  module Retryable

    # The abstract superclass for workers employed by Mongo::Retryable.
    #
    # @api private
    class BaseWorker
      extend Forwardable

      def_delegators :retryable,
        :client,
        :cluster,
        :select_server

      # @return [ Mongo::Retryable ] retryable A reference to the client object
      #   that instatiated this worker.
      attr_reader :retryable

      # Constructs a new worker.
      #
      # @example Instantiating a new read worker
      #   worker = Mongo::Retryable::ReadWorker.new(self)
      #
      # @example Instantiating a new write worker
      #   worker = Mongo::Retryable::WriteWorker.new(self)
      #
      # @param [ Mongo::Retryable ] retryable The client object that is using
      #   this worker to perform a retryable operation
      def initialize(retryable)
        @retryable = retryable
      end

      private

      # Indicate which exception classes that are generally retryable. 
      #
      # @return [ Array<Mongo:Error> ] Array of exception classes that are
      #   considered retryable.
      def retryable_exceptions
        [
          Error::ConnectionPerished,
          Error::ServerNotUsable,
          Error::SocketError,
          Error::SocketTimeoutError
        ].freeze
      end

      # Tests to see if the given exception instance is of a type that can
      # be retried.
      #
      # @return [ true | false ] true if the exception is retryable.
      def is_retryable_exception?(e)
        retryable_exceptions.any? { |klass| klass === e }
      end

      # Logs the given deprecation warning the first time it is called for a
      # given key; after that, it does nothing when given the same key.
      def deprecation_warning(key, warning)
        $_deprecation_warnings ||= {}
        unless $_deprecation_warnings[key]
          $_deprecation_warnings[key] = true
          Logger.logger.warn(warning)
        end
      end

      # Log a warning so that any application slow down is immediately obvious.
      def log_retry(e, options = nil)
        message = (options || {}).fetch(:message, "Retry")
        Logger.logger.warn "#{message} due to: #{e.class.name}: #{e.message}"
      end
    end

  end
end
