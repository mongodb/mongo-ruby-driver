# Copyright (C) 2014-2019 MongoDB, Inc.
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
  module Event

    # This handles a change in description.
    #
    # @since 2.0.6
    class DescriptionChanged < Base

      # @return [ Mongo::Cluster ] cluster The cluster.
      attr_reader :cluster

      # Initialize the new host added event handler.
      #
      # @example Create the new handler.
      #   ServerAdded.new(cluster)
      #
      # @param [ Mongo::Cluster ] cluster The cluster to publish from.
      #
      # @since 2.0.0
      def initialize(cluster)
        @cluster = cluster
      end

      # This event publishes an event to add the cluster and logs the
      # configuration change.
      #
      # @param [ Server::Description ] previous_desc Previous server description.
      # @param [ Server::Description ] updated_desc The changed description.
      #
      # @since 2.0.0
      def handle(previous_desc, updated_desc)
        cluster.sdam_flow_lock.synchronize do
          Mongo::Cluster::SdamFlow.new(cluster, previous_desc, updated_desc).server_description_changed
        end
      end
    end
  end
end
