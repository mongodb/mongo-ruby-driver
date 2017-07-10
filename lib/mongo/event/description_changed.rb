
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
    class DescriptionChanged
      include Monitoring::Publishable

      # @return [ Mongo::Cluster ] cluster The cluster.
      attr_reader :cluster

      # @return [ Hash ] options The options.
      attr_reader :options

      # @return [ Monitoring ] monitoring The monitoring.
      attr_reader :monitoring

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
        @options = cluster.options
        @monitoring = cluster.monitoring
      end

      # This event publishes an event to add the cluster and logs the
      # configuration change.
      #
      # @example Handle the event.
      #   server_added.handle('127.0.0.1:27018')
      #
      # @param [ Server::Description ] updated The changed description.
      #
      # @since 2.0.0
      def handle(previous, updated)
        publish_sdam_event(
          Monitoring::SERVER_DESCRIPTION_CHANGED,
          Monitoring::Event::ServerDescriptionChanged.new(
            updated.address,
            cluster.topology,
            previous,
            updated
          )
        )
        cluster.add_hosts(updated)
        cluster.remove_hosts(updated)
      end
    end
  end
end
