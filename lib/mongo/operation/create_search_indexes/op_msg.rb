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
        # @param [ Connection ] _connection the connection that will receive the
        #   command
        #
        # @return [ Hash ] the selector
        def selector(_connection)
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
