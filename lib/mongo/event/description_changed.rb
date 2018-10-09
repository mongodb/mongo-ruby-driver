# Copyright (C) 2014-2018 MongoDB, Inc.
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

        if cluster.topology.is_a?(::Mongo::Cluster::Topology::Unknown) && updated.replica_set_name && updated.replica_set_name != ''
          old_topology = cluster.topology
          new_cls = if updated.primary?
            ::Mongo::Cluster::Topology::ReplicaSetWithPrimary
          else
            ::Mongo::Cluster::Topology::ReplicaSetNoPrimary
          end
          new_topology = new_cls.new(
            cluster.topology.options.merge(
              replica_set: updated.replica_set_name,
            ), cluster.topology.monitoring)
          cluster.send(:instance_variable_set, '@topology', new_topology)
          publish_sdam_event(
            Monitoring::TOPOLOGY_CHANGED,
            Monitoring::Event::TopologyChanged.new(
              old_topology, new_topology,
            )
          )
        elsif cluster.topology.is_a?(Cluster::Topology::ReplicaSetWithPrimary) && updated.unknown?
          # here the unknown server is already removed from the topology
          # TODO this is a checkIfHasPrimary implementation, move/refactor it
          # as part of https://jira.mongodb.org/browse/RUBY-1492
          unless cluster.servers.any?(&:primary?)
            old_topology = cluster.topology
            new_topology = Cluster::Topology::ReplicaSetNoPrimary.new(
              cluster.topology.options, cluster.topology.monitoring)
            cluster.send(:instance_variable_set, '@topology', new_topology)
            publish_sdam_event(
              Monitoring::TOPOLOGY_CHANGED,
              Monitoring::Event::TopologyChanged.new(
                old_topology, new_topology,
              )
            )
          end
        end
      end
    end
  end
end
