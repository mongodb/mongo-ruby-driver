# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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
  class Error

    # Exception raised if an invalid operation is attempted as part of a transaction.
    #
    # @since 2.6.0
    class InvalidTransactionOperation < Error

      # The error message for when a user attempts to commit or abort a transaction when none is in
      # progress.
      #
      # @since 2.6.0
      NO_TRANSACTION_STARTED = 'no transaction started'.freeze

      # The error message for when a user attempts to start a transaction when one is already in
      # progress.
      #
      # @since 2.6.0.
      TRANSACTION_ALREADY_IN_PROGRESS = 'transaction already in progress'.freeze

      # The error message for when a transaction read operation uses a non-primary read preference.
      #
      # @since 2.6.0
      INVALID_READ_PREFERENCE = 'read preference in a transaction must be primary'.freeze

      # The error message for when a transaction is started with an unacknowledged write concern.
      #
      # @since 2.6.0
      UNACKNOWLEDGED_WRITE_CONCERN = 'transactions do not support unacknowledged write concern'.freeze

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::Error::InvalidTransactionOperation.new(msg)
      #
      # @since 2.6.0
      def initialize(msg)
        super(msg)
      end

      # Create an error message for incorrectly running a transaction operation twice.
      #
      # @example Create the error message.
      #   InvalidTransactionOperation.cannot_call_twice(op)
      #
      # @param [ Symbol ] op The operation which was run twice.
      #
      # @since 2.6.0
      def self.cannot_call_twice_msg(op)
        "cannot call #{op} twice"
      end

      # Create an error message for incorrectly running a transaction operation that cannot be run
      # after the previous one.
      #
      # @example Create the error message.
      #   InvalidTransactionOperation.cannot_call_after(last_op, current_op)
      #
      # @param [ Symbol ] last_op The operation which was run before.
      # @param [ Symbol ] current_op The operation which cannot be run.
      #
      # @since 2.6.0
      def self.cannot_call_after_msg(last_op, current_op)
        "Cannot call #{current_op} after calling #{last_op}"
      end
    end
  end
end
