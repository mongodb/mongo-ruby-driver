# frozen_string_literal: true

module Mongo
  module Operation
    # Defines the behavior of operations that have the default timeout
    # behavior described by the client-side operation timeouts (CSOT)
    # spec.
    #
    # @api private
    module Timed
      # If a timeout is active (as defined by the context), and it has not
      # yet expired, add :maxTimeMS to the spec.
      #
      # @param [ Hash ] spec The spec to modify
      # @param [ Connection ] connection The connection that will be used to
      #   execute the operation
      # @param [ Operation::Context ] context The active context
      #
      # @return [ Hash ] the spec
      #
      # @raises [ Mongo::Error::TimeoutError ] if the current timeout has
      #   expired.
      def apply_relevant_timeouts_to(spec, connection, context)
        with_max_time(connection, context) do |max_time_sec|
          return spec if max_time_sec.nil?

          spec.tap { spec[:maxTimeMS] = (max_time_sec * 1_000).to_i }
        end
      end

      # A helper method that computes the remaining timeout (in seconds) and
      # yields it to the associated block. If no timeout is present, yields
      # nil. If the timeout has expired, raises Mongo::Error::TimeoutError.
      #
      # @param [ Connection ] connection The connection that will be used to
      #   execute the operation
      # @param [ Operation::Context ] context The active context
      #
      # @return [ Hash ] the result of yielding to the block (which must be
      #   a Hash)
      def with_max_time(connection, context)
        if context.remaining_timeout_sec.nil?
          yield nil
        else
          max_time_sec = context.remaining_timeout_sec - connection.server.minimum_round_trip_time
          raise Mongo::Error::TimeoutError if max_time_sec <= 0

          yield max_time_sec
        end
      end
    end
  end
end
