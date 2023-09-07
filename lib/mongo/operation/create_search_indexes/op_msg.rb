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
        # @param [ Mongo::Server ] _server the server that will receive the
        #   command
        #
        # @return [ Hash ] the selector
        def selector(_server)
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
