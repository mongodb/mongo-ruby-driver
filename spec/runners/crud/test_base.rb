module Mongo
  module CRUD

    class CRUDTestBase

      # The test description.
      #
      # @return [ String ] description The test description.
      attr_reader :description

      # The expected command monitoring events.
      attr_reader :expectations

      private

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

      # If the deployment is a sharded cluster, creates a direct client
      # to each of the mongos nodes and yields each in turn to the
      # provided block. Does nothing in other topologies.
      def mongos_each_direct_client
        if ClusterConfig.instance.topology == :sharded
          client = ClientRegistry.instance.global_client('basic')
          client.cluster.next_primary
          client.cluster.servers.each do |server|
            direct_client = ClientRegistry.instance.new_local_client(
              [server.address.to_s],
              SpecConfig.instance.test_options.merge(
                connect: :sharded
              ).merge(SpecConfig.instance.auth_options))
            yield direct_client
            direct_client.close
          end
        end
      end
    end
  end
end
