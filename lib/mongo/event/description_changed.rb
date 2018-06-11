
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
  module Event

    # This handles a change in description.
    #
    # @since 2.0.6
    class DescriptionChanged < Base
      include Monitoring::Publishable

      # @return [ Mongo::Cluster ] cluster The cluster.
      attr_reader :cluster

      # @return [ Hash ] options The options.
      attr_reader :options

      # @return [ Monitoring ] monitoring The monitoring.
      attr_reader :monitoring

      # Initialize the description changed event handler.
      #
      # @example Create the new handler.
      #   DescriptionChanged.new(cluster)
      #
      # @param [ Mongo::Cluster ] cluster The cluster to publish from.
      #
      # @since 2.0.0
      def initialize(cluster)
        @cluster = cluster
        @options = cluster.options
        @monitoring = cluster.monitoring
      end

      # This event publishes an event to add the cluster and logs the
      # configuration change.
      #
      # @example Handle the event.
      #   description_changed.handle('127.0.0.1:27018')
      #
      # @param [ Server::Description ] updated The changed description.
      #
      # @since 2.0.0
      def handle(server, previous, updated)
        cluster.server_description_changed(server, previous, updated)

        # The SERVER_DESCRIPTION_CHANGED event is only used for logging,
        # all SDAM logic is in the server_description_changed method call
        # above
        publish_sdam_event(
          Monitoring::SERVER_DESCRIPTION_CHANGED,
          Monitoring::Event::ServerDescriptionChanged.new(
            updated.address,
            cluster.topology,
            previous,
            updated
          )
        )

        return
        cluster.add_hosts(updated)
        cluster.remove_hosts(updated)
      end
    end
  end
end
