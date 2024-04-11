# frozen_string_literal: true

module Mongo
  module Operation
    class CreateSearchIndexes
      # A MongoDB createSearchIndexes operation sent as an op message.
      #
      # @api private
      class OpMsg < OpMsgBase
        include ExecutableTransactionLabel

        private

        # Returns the command to send to the database, describing the
        # desired createSearchIndexes operation.
        #
        # @param [ Connection ] _connection the connection that the command
        #   will be executed on.
        # @param [ Operation::Context ] _context the operation context that
        #   is active for the command.
        #
        # @return [ Hash ] the selector
        def selector(_connection, _context)
          {
            createSearchIndexes: coll_name,
            :$db => db_name,
            indexes: indexes,
          }
        end
      end
    end
  end
end
