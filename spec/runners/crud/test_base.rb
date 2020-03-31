module Mongo
  module CRUD

    class CRUDTestBase

      # The test description.
      #
      # @return [ String ] description The test description.
      attr_reader :description

      # The expected command monitoring events.
      attr_reader :expectations

      def resolve_target(client, operation)
        case operation.object
        when 'collection'
          client[@spec.collection_name].with(operation.collection_options)
        when 'database'
          client.database
        when 'client'
          client
        when 'gridfsbucket'
          client.database.fs
        else
          raise "Unknown target #{operation.object}"
        end
      end
    end
  end
end
