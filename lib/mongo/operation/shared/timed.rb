# frozen_string_literal: true

module Mongo
  module Operation
    # Defines the behavior of operations that have the default timeout
    # behavior described by the client-side operation timeouts (CSOT)
    # spec.
    #
    # @api private
    module Timed
      # If a timeout is active (as defined by the current context), and it has
      # not yet expired, add :maxTimeMS to the spec.
      #
      # @param [ Hash ] spec The spec to modify
      # @param [ Connection ] connection The connection that will be used to
      #   execute the operation
      #
      # @return [ Hash ] the spec
      #
      # @raises [ Mongo::Error::TimeoutError ] if the current timeout has
      #   expired.
      def apply_relevant_timeouts_to(spec, connection)
        with_max_time(connection) do |max_time_sec|
          return spec if max_time_sec.nil?
          return spec if connection.description.mongocryptd?

          spec.tap { spec[:maxTimeMS] = (max_time_sec * 1_000).to_i }
        end
      end

      # A helper method that computes the remaining timeout (in seconds) and
      # yields it to the associated block. If no timeout is present, yields
      # nil. If the timeout has expired, raises Mongo::Error::TimeoutError.
      #
      # @param [ Connection ] connection The connection that will be used to
      #   execute the operation
      #
      # @return [ Hash ] the result of yielding to the block (which must be
      #   a Hash)
      def with_max_time(connection)
        if context&.timeout?
          max_time_sec = context.remaining_timeout_sec - connection.server.minimum_round_trip_time
          raise Mongo::Error::TimeoutError if max_time_sec <= 0

          yield max_time_sec
        else
          yield nil
        end
      end
    end
  end
end
