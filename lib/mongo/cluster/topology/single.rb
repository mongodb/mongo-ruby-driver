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
  class Cluster
    module Topology

      # Defines behaviour for when a cluster is in single topology.
      #
      # @since 2.0.0
      class Single
        include Monitoring::Publishable

        # The display name for the topology.
        #
        # @since 2.0.0
        NAME = 'Single'.freeze

        # @return [ Hash ] options The options.
        attr_reader :options

        # @return [ String ] seed The seed address.
        attr_reader :seed

        # @return [ monitoring ] monitoring the monitoring.
        attr_reader :monitoring

        # Get the display name.
        #
        # @example Get the display name.
        #   Single.display_name
        #
        # @return [ String ] The display name.
        #
        # @since 2.0.0
        def display_name
          NAME
        end

        # Elect a primary server within this topology.
        #
        # @example Elect a primary server.
        #   topology.elect_primary(description, servers)
        #
        # @param [ Server::Description ] description The description of the
        #   elected primary.
        # @param [ Array<Server> ] servers The list of known servers to the
        #   cluster.
        #
        # @return [ Single ] The topology.
        def elect_primary(description, servers); self; end

        # Determine if the topology would select a readable server for the
        # provided candidates and read preference.
        #
        # @example Is a readable server present?
        #   topology.has_readable_server?(cluster, server_selector)
        #
        # @param [ Cluster ] cluster The cluster.
        # @param [ ServerSelector ] server_selector The server
        #   selector.
        #
        # @return [ true ] A standalone always has a readable server.
        #
        # @since 2.4.0
        def has_readable_server?(cluster, server_selector = nil); true; end

        # Determine if the topology would select a writable server for the
        # provided candidates.
        #
        # @example Is a writable server present?
        #   topology.has_writable_server?(servers)
        #
        # @param [ Cluster ] cluster The cluster.
        #
        # @return [ true ] A standalone always has a writable server.
        #
        # @since 2.4.0
        def has_writable_server?(cluster); true; end

        # Initialize the topology with the options.
        #
        # @example Initialize the topology.
        #   Single.new(options)
        #
        # @param [ Hash ] options The options.
        # @param [ Monitoring ] monitoring The monitoring.
        # @param [ Array<String> ] seeds The seeds.
        #
        # @since 2.0.0
        def initialize(options, monitoring, seeds = [])
          @options = options
          @monitoring = monitoring
          @seed = seeds.first
        end

        # A single topology is not a replica set.
        #
        # @example Is the topology a replica set?
        #   Single.replica_set?
        #
        # @return [ false ] Always false.
        #
        # @since 2.0.0
        def replica_set?; false; end

        # Single topologies have no replica set name.
        #
        # @example Get the replica set name.
        #   single.replica_set_name
        #
        # @return [ nil ] Always nil.
        #
        # @since 2.0.0
        def replica_set_name; nil; end

        # Select appropriate servers for this topology.
        #
        # @example Select the servers.
        #   Single.servers(servers, 'test')
        #
        # @param [ Array<Server> ] servers The known servers.
        #
        # @return [ Array<Server> ] The single servers.
        #
        # @since 2.0.0
        def servers(servers, name = nil)
          [ servers.detect { |server| !server.unknown? } ]
        end

        # Whether a server description's hosts may be added to the cluster.
        #
        # @example Check if a description's hosts may be added to the cluster.
        #   topology.add_hosts?(description, servers)
        #
        # @param [ Mongo::Server::Description ] description The description.
        # @param [ Array<Mongo::Server> ] servers The cluster servers.
        #
        # @return [ false ] A description's hosts are never added to a
        #   cluster of Single topology.
        #
        # @since 2.0.6
        def add_hosts?(description, servers); false; end

        # Whether a description can be used to remove hosts from the cluster.
        #
        # @example Check if a description can be used to remove hosts from
        # the cluster.
        #   topology.remove_hosts?(description)
        #
        # @param [ Mongo::Server::Description ] description The description.
        #
        # @return [ true ] A description can never be used to remove hosts
        #   from a cluster of Single topology.
        #
        # @since 2.0.6
        def remove_hosts?(description); false; end

        # Whether a specific server in the cluster can be removed, given a description.
        #
        # @example Check if a specific server can be removed from the cluster.
        #   topology.remove_server?(description, server)
        #
        # @param [ Mongo::Server::Description ] description The description.
        # @param [ Mongo::Serve ] server The server in question.
        #
        # @return [ false ] A server is never removed from a cluster of Single topology.
        #
        # @since 2.0.6
        def remove_server?(description, server); false; end

        # A single topology is not sharded.
        #
        # @example Is the topology sharded?
        #   Single.sharded?
        #
        # @return [ false ] Always false.
        #
        # @since 2.0.0
        def sharded?; false; end

        # A single topology is single.
        #
        # @example Is the topology single?
        #   Single.single?
        #
        # @return [ true ] Always true.
        #
        # @since 2.0.0
        def single?; true; end

        # An single topology is not unknown.
        #
        # @example Is the topology unknown?
        #   Single.unknown?
        #
        # @return [ false ] Always false.
        #
        # @since 2.0.0
        def unknown?; false; end

        # Notify the topology that a standalone was discovered.
        #
        # @example Notify the topology that a standalone was discovered.
        #   topology.standalone_discovered
        #
        # @return [ Topology::Single ] Always returns self.
        #
        # @since 2.0.6
        def standalone_discovered; self; end

        # Publish that a member of this topology was discovered.
        #
        # @example Publish that a member was discovered.
        #   topology.member_discovered
        #
        # @since 2.4.0
        def member_discovered
          publish_sdam_event(
            Monitoring::TOPOLOGY_CHANGED,
            Monitoring::Event::TopologyChanged.new(self, self)
          )
        end
      end
    end
  end
end
