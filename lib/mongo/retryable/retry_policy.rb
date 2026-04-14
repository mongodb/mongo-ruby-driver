# frozen_string_literal: true

module Mongo
  module Retryable
    # Encapsulates the retry policy for client backpressure with
    # exponential backoff and jitter.
    #
    # One instance is created per Client and shared across all operations
    # on that client.
    #
    # @api private
    class RetryPolicy
      # @return [ Integer ] The maximum number of overload retries.
      attr_reader :max_retries

      # Create a new retry policy.
      #
      # @param [ Integer ] max_retries The maximum number of overload
      #   retry attempts. Defaults to Backpressure::DEFAULT_MAX_RETRIES.
      def initialize(max_retries: Backpressure::DEFAULT_MAX_RETRIES)
        @max_retries = max_retries
      end

      # Calculate the backoff delay for a given retry attempt.
      #
      # @param [ Integer ] attempt The retry attempt number (1-indexed).
      # @param [ Float ] jitter A random float in [0.0, 1.0).
      #
      # @return [ Float ] The backoff delay in seconds.
      def backoff_delay(attempt, jitter: rand)
        Backpressure.backoff_delay(attempt, jitter: jitter)
      end

      # Determine whether an overload retry should be attempted.
      #
      # @param [ Integer ] attempt The retry attempt number (1-indexed).
      # @param [ Float ] delay The backoff delay in seconds.
      # @param [ Mongo::Operation::Context | nil ] context The operation
      #   context (for CSOT deadline checking).
      #
      # @return [ true | false ] Whether the retry should proceed.
      def should_retry_overload?(attempt, delay, context: nil)
        return false if attempt > @max_retries
        return false if exceeds_deadline?(delay, context)

        true
      end

      private

      def exceeds_deadline?(delay, context)
        return false unless context&.csot?

        deadline = context&.deadline
        deadline&.nonzero? && Utils.monotonic_time + delay > deadline
      end
    end
  end
end
