# Copyright (C) 2013 10gen Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
      @connect_mutex.synchronize do
        begin
          thread_local[:locks][:connecting_manager] = true
          @refresh_required = false
          disconnect_old_members
          connect_to_members
          initialize_pools best(@members)
          update_max_sizes
          @seeds = discovered_seeds
        ensure
          thread_local[:locks][:connecting_manager] = false
        end
      end
    end

    # Checks that each node is healthy (via check_is_master) and that each
    # node is in fact a mongos. If either criteria are not true, a refresh is
    # set to be triggered and close() is called on the node.
    #
    # @return [Boolean] indicating if a refresh is required.
    def check_connection_health
      @refresh_required = false
      @members.each do |member|
        begin
          config = @client.check_is_master([member.host, member.port])
          unless config && config.has_key?('msg')
            @refresh_required = true
            member.close
          end
        rescue OperationTimeout
          @refresh_required = true
          member.close
        end
        break if @refresh_required
      end
      @refresh_required
    end

  end
end
