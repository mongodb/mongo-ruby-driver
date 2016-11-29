# Copyright (C) 2015 MongoDB, Inc.
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

    # This handles member discovered events for server descriptions.
    #
    # @since 2.4.0
    class MemberDiscovered
      include Monitoring::Publishable

      # @return [ Mongo::Cluster ] cluster The cluster.
      attr_reader :cluster

      # @return [ Hash ] options The options.
      attr_reader :options

      # @return [ Monitoring ] monitoring The monitoring.
      attr_reader :monitoring

      # Initialize the new member discovered event handler.
      #
      # @example Create the new handler.
      #   MemberDiscovered.new(cluster)
      #
      # @param [ Mongo::Cluster ] cluster The cluster to publish from.
      #
      # @since 2.0.0
      def initialize(cluster)
        @cluster = cluster
        @options = cluster.options
        @monitoring = cluster.monitoring
      end

      # This event tells the cluster that a member of a topology is discovered.
      #
      # @example Handle the event.
      #   member_discovered.handle(previous_description, description)
      #
      # @param [ Server::Description ] previous The previous description of the server.
      # @param [ Server::Description ] updated The updated description of the server.
      #
      # @since 2.4.0
      def handle(previous, updated)
        if updated.primary? || updated.mongos?
          cluster.elect_primary!(updated)
        else
          cluster.member_discovered
        end
      end
    end
  end
end
