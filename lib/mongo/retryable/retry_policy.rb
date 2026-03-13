# frozen_string_literal: true

module Mongo
  module Retryable
    # Encapsulates the retry policy for client backpressure, combining
    # exponential backoff with jitter and an optional token bucket for
    # adaptive retries.
    #
    # One instance is created per Client and shared across all operations
    # on that client.
    #
    # @api private
    class RetryPolicy
      # Create a new retry policy.
      #
      # @param [ true | false ] adaptive_retries Whether the adaptive
      #   retry token bucket is enabled.
      def initialize(adaptive_retries: false)
        @token_bucket = adaptive_retries ? TokenBucket.new : nil
      end

      # @return [ TokenBucket | nil ] The token bucket, if adaptive
      #   retries are enabled.
      attr_reader :token_bucket

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
      # Checks that the attempt number does not exceed MAX_RETRIES,
      # that the backoff delay would not exceed the CSOT deadline (if set),
      # and that a token is available (if adaptive retries are enabled).
      #
      # @param [ Integer ] attempt The retry attempt number (1-indexed).
      # @param [ Float ] delay The backoff delay in seconds.
      # @param [ Mongo::CsotTimeoutHolder | nil ] context The operation
      #   context (for CSOT deadline checking).
      #
      # @return [ true | false ] Whether the retry should proceed.
      def should_retry_overload?(attempt, delay, context: nil)
        return false if attempt > Backpressure::MAX_RETRIES

        if context&.csot? && context&.deadline
          return false if context.deadline.nonzero? &&
            Utils.monotonic_time + delay > context.deadline
        end

        if @token_bucket && !@token_bucket.consume(1)
          return false
        end

        true
      end

      # Record a successful operation by depositing tokens into the
      # bucket.
      #
      # @param [ true | false ] is_retry Whether the success came from
      #   a retried attempt.
      def record_success(is_retry:)
        return unless @token_bucket

        tokens = Backpressure::RETRY_TOKEN_RETURN_RATE
        tokens += 1 if is_retry
        @token_bucket.deposit(tokens)
      end

      # Record a non-overload failure during a retry attempt by
      # depositing 1 token.
      def record_non_overload_retry_failure
        @token_bucket&.deposit(1)
      end
    end
  end
end
