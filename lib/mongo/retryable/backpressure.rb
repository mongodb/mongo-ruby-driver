# frozen_string_literal: true

module Mongo
  module Retryable
    # Constants and helpers for client backpressure (exponential backoff
    # and jitter in retry loops).
    #
    # @api private
    module Backpressure
      # Base backoff delay in seconds.
      BASE_BACKOFF = 0.1

      # Maximum backoff delay in seconds.
      MAX_BACKOFF = 10

      # Default maximum number of retries for overload errors.
      DEFAULT_MAX_RETRIES = 2

      # Calculate the backoff delay for a given retry attempt.
      #
      # @param [ Integer ] attempt The retry attempt number (1-indexed).
      # @param [ Float ] jitter A random float in [0.0, 1.0). Defaults to
      #   a random value. Can be injected for deterministic testing.
      #
      # @return [ Float ] The backoff delay in seconds.
      def self.backoff_delay(attempt, jitter: rand)
        jitter * [ MAX_BACKOFF, BASE_BACKOFF * (2**(attempt - 1)) ].min
      end
    end
  end
end
