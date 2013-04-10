
module Mongo
  class ShardingPoolManager < PoolManager
    def inspect
      "<Mongo::ShardingPoolManager:0x#{self.object_id.to_s(16)} @seeds=#{@seeds}>"
    end

    # "Best" should be the member with the fastest ping time
    # but connect/connect_to_members reinitializes @members
    def best(members)
      Array(members.first)
    end

    def connect
      @refresh_required = false
      disconnect_old_members
      connect_to_members
      initialize_pools best(@members)
      @seeds = discovered_seeds
    end

    # We want to refresh to the member with the fastest ping time
    # but also want to minimize refreshes
    # We're healthy if the primary is pingable. If this isn't the case,
    # or the members have changed, set @refresh_required to true, and return.
    # The config.mongos find can't be part of the connect call chain due to infinite recursion
    def check_connection_health
      begin
        seeds = @client['config']['mongos'].find.map do |mongos|
                  Support.normalize_seeds(mongos['_id'])
                end
        if discovered_seeds != seeds
          @seeds = seeds
          @refresh_required = true
        end
      rescue Mongo::OperationFailure
        @refresh_required = true
      end
    end
  end
end
