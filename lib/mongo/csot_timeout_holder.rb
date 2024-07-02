# frozen_string_literal: true

# Copyright (C) 2024 MongoDB Inc.
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
  # This class stores operation timeout and provides corresponding helper methods.
  #
  # @api private
  class CsotTimeoutHolder
    def initialize(session: nil, operation_timeouts: {})
      @deadline = calculate_deadline(operation_timeouts, session)
      @operation_timeouts = operation_timeouts
      @timeout_sec = (@deadline - Utils.monotonic_time if @deadline)
    end

    attr_reader :deadline, :timeout_sec, :operation_timeouts

    # @return [ true | false ] Whether CSOT is enabled for the operation
    def csot?
      !deadline.nil?
    end

    # @return [ true | false ] Returns false if CSOT is not enabled, or if
    #   CSOT is set to 0 (means unlimited), otherwise true.
    def timeout?
      ![ nil, 0 ].include?(@deadline)
    end

    # @return [ Float | nil ] Returns the remaining seconds of the timeout
    #   set for the operation; if no timeout is set, or the timeout is 0
    #   (means unlimited) returns nil.
    def remaining_timeout_sec
      return nil unless timeout?

      deadline - Utils.monotonic_time
    end

    def remaining_timeout_sec!
      check_timeout!
      remaining_timeout_sec
    end

    # @return [ Integer | nil ] Returns the remaining milliseconds of the timeout
    #   set for the operation; if no timeout is set, or the timeout is 0
    #   (means unlimited) returns nil.
    def remaining_timeout_ms
      seconds = remaining_timeout_sec
      return nil if seconds.nil?

      (seconds * 1_000).to_i
    end

    def remaining_timeout_ms!
      check_timeout!
      remaining_timeout_ms
    end

    # @return [ true | false ] Whether the timeout for the operation expired.
    #   If no timeout set, this method returns false.
    def timeout_expired?
      if timeout?
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
      return unless timeout_expired?

      raise Error::TimeoutError, "Operation took more than #{timeout_sec} seconds"
    end

    private

    def calculate_deadline(opts = {}, session = nil)
      check_no_override_inside_transaction!(opts, session)
      return session&.with_transaction_deadline if session&.with_transaction_deadline

      if (operation_timeout_ms = opts[:operation_timeout_ms])
        calculate_deadline_from_timeout_ms(operation_timeout_ms)
      elsif (inherited_timeout_ms = opts[:inherited_timeout_ms])
        calculate_deadline_from_timeout_ms(inherited_timeout_ms)
      end
    end

    def check_no_override_inside_transaction!(opts, session)
      return unless opts[:operation_timeout_ms] && session&.with_transaction_deadline

      raise ArgumentError, 'Cannot override timeout_ms inside with_transaction block'
    end

    def calculate_deadline_from_timeout_ms(operation_timeout_ms)
      if operation_timeout_ms.positive?
        Utils.monotonic_time + (operation_timeout_ms / 1_000.0)
      elsif operation_timeout_ms.zero?
        0
      elsif operation_timeout_ms.negative?
        raise ArgumentError, "timeout_ms must be a non-negative integer but #{operation_timeout_ms} given"
      end
    end
  end
end
