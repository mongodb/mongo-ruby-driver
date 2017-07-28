# Copyright (C) 2014-2017 MongoDB, Inc.
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
  module Operation

    # Adds cluster time to selectors sent to sharded clusters for server versions >= 3.6.
    #
    # @since 2.5.0
    module ClusterTime

      # Behavior for adding $clusterTime to commands.
      #
      # @since 2.5.0
      CLUSTER_TIME = :'$clusterTime'.freeze

      private

      def update_selector_with_cluster_time(sel, server)
        if server.mongos? && server.cluster_time
          sel[CLUSTER_TIME] = server.cluster_time
        end
        sel
      end
    end
  end
end
