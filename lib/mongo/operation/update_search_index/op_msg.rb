# frozen_string_literal: true

module Mongo
  module Operation
    class UpdateSearchIndex
      # A MongoDB updateSearchIndex operation sent as an op message.
      #
      # @api private
      class OpMsg < OpMsgBase
        include ExecutableTransactionLabel

        private

        # Returns the command to send to the database, describing the
        # desired updateSearchIndex operation.
        #
        # @param [ Connection ] _connection the connection that will serve the
        #   command
        # @param [ Operation::Context ] _context the context that is active for
        #   the operation
        #
        # @return [ Hash ] the selector
        def selector(_connection, _context)
          {
            updateSearchIndex: coll_name,
            :$db => db_name,
            definition: index,
          }.tap do |sel|
            sel[:id] = index_id if index_id
            sel[:name] = index_name if index_name
          end
        end
      end
    end
  end
end
