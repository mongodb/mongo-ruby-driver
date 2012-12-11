
module Mongo
  class ShardingPoolManager < PoolManager

    attr_reader :client, :primary, :primary_pool, :hosts, :nodes,
      :max_bson_size, :members

    # Create a new set of connection pools.
    #
    # The pool manager will by default use the original seed list passed
    # to the connection objects, accessible via connection.seeds. In addition,
    # the user may pass an additional list of seeds nodes discovered in real
    # time. The union of these lists will be used when attempting to connect,
    # with the newly-discovered nodes being used first.
    def initialize(client, seeds=[])
      super
    end

    def inspect
      "<Mongo::ShardingPoolManager:0x#{self.object_id.to_s(16)} @seeds=#{@seeds}>"
    end

    # "Best" should be the member with the fastest ping time
    # but connect/connect_to_members reinitializes @members
    def best(members)
      Array(members.first)
    end

    def connect
      close if @previously_connected

      initialize_data
      members = connect_to_members
      initialize_pools(best(members))

      @members = members
      @previously_connected = true
    end

    # We want to refresh to the member with the fastest ping time
    # but also want to minimize refreshes
    # We're healthy if the primary is pingable. If this isn't the case,
    # or the members have changed, set @refresh_required to true, and return.
    # The config.mongos find can't be part of the connect call chain due to infinite recursion
    def check_connection_health
      begin
        seeds = @client['config']['mongos'].find.to_a.map{|doc| doc['_id']}
        if @seeds != seeds
          @seeds = seeds
          @refresh_required = true
        end
      rescue Mongo::OperationFailure
        @refresh_required = true
      end
    end
  end
end
