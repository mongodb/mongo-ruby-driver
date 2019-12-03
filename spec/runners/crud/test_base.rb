module Mongo
  module CRUD

    class CRUDTestBase

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
