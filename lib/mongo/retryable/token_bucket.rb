# frozen_string_literal: true

module Mongo
  module Retryable
    # A thread-safe token bucket for rate limiting retries during server
    # overload. Used by the adaptive retry mechanism.
    #
    # @api private
    class TokenBucket
      # Create a new token bucket.
      #
      # @param [ Float ] capacity The maximum number of tokens the bucket
      #   can hold. Defaults to Backpressure::DEFAULT_RETRY_TOKEN_CAPACITY.
      def initialize(capacity: Backpressure::DEFAULT_RETRY_TOKEN_CAPACITY)
        @capacity = capacity.to_f
        @tokens = @capacity
        @mutex = Mutex.new
      end

      # @return [ Float ] The maximum capacity of the bucket.
      attr_reader :capacity

      # Return the current number of tokens.
      #
      # @return [ Float ] The current token count.
      def tokens
        @mutex.synchronize { @tokens }
      end

      # Consume n tokens from the bucket.
      #
      # @param [ Float ] n The number of tokens to consume.
      #
      # @return [ true | false ] true if the tokens were consumed,
      #   false if there were insufficient tokens.
      def consume(n = 1)
        @mutex.synchronize do
          if @tokens >= n
            @tokens -= n
            true
          else
            false
          end
        end
      end

      # Deposit n tokens into the bucket, up to the maximum capacity.
      #
      # @param [ Float ] n The number of tokens to deposit.
      #
      # @return [ Float ] The new token count.
      def deposit(n)
        @mutex.synchronize do
          @tokens = [ @capacity, @tokens + n ].min
        end
      end
    end
  end
end
