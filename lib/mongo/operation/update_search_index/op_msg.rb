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
        # @param [ Mongo::Server ] _server the server that will receive the
        #   command
        #
        # @return [ Hash ] the selector
        def selector(_server)
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
