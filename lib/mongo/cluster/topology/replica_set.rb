# Copyright (C) 2014-2015 MongoDB, Inc.
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

      # Defines behaviour when a cluster is in replica set topology.
      #
      # @since 2.0.0
      class ReplicaSet
        include Loggable

        # Constant for the replica set name configuration option.
        #
        # @since 2.0.0
        REPLICA_SET_NAME = :replica_set.freeze

        # @return [ Hash ] options The options.
        attr_reader :options

        # The display name for the topology.
        #
        # @since 2.0.0
        NAME = 'Replica Set'.freeze

        # Get the display name.
        #
        # @example Get the display name.
        #   ReplicaSet.display_name
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
        # @return [ ReplicaSet ] The topology.
        def elect_primary(description, servers)
          if description.replica_set_name == replica_set_name
            unless detect_stale_primary!(description)
              log_debug("Server #{description.address.to_s} elected as primary in #{replica_set_name}.")
              servers.each do |server|
                if server.primary? && server.address != description.address
                  server.description.unknown!
                end
              end
              update_max_election_id(description)
              update_max_set_version(description)
            end
          else
            log_warn(
              "Server #{description.address.to_s} has incorrect replica set name: " +
              "'#{description.replica_set_name}'. The current replica set name is '#{replica_set_name}'."
            )
          end
          self
        end

        # Initialize the topology with the options.
        #
        # @example Initialize the topology.
        #   ReplicaSet.new(options)
        #
        # @param [ Hash ] options The options.
        #
        # @since 2.0.0
        def initialize(options, seeds = [])
          @options = options
          @max_election_id = nil
          @max_set_version = nil
        end

        # A replica set topology is a replica set.
        #
        # @example Is the topology a replica set?
        #   ReplicaSet.replica_set?
        #
        # @return [ true ] Always true.
        #
        # @since 2.0.0
        def replica_set?; true; end

        # Get the replica set name configured for this topology.
        #
        # @example Get the replica set name.
        #   topology.replica_set_name
        #
        # @return [ String ] The name of the configured replica set.
        #
        # @since 2.0.0
        def replica_set_name
          @replica_set_name ||= options[REPLICA_SET_NAME]
        end

        # Select appropriate servers for this topology.
        #
        # @example Select the servers.
        #   ReplicaSet.servers(servers)
        #
        # @param [ Array<Server> ] servers The known servers.
        #
        # @return [ Array<Server> ] The servers in the replica set.
        #
        # @since 2.0.0
        def servers(servers)
          servers.select do |server|
            (replica_set_name.nil? || server.replica_set_name == replica_set_name) &&
              server.primary? || server.secondary?
          end
        end

        # Whether a server description's hosts may be added to the cluster.
        #
        # @example Check if a description's hosts may be added to the cluster.
        #   topology.add_hosts?(description, servers)
        #
        # @param [ Mongo::Server::Description ] description The description.
        # @param [ Array<Mongo::Server> ] servers The cluster servers.
        #
        # @return [ true, false ] Whether a description's hosts may be added.
        #
        # @since 2.0.6
        def add_hosts?(description, servers)
          !!(member_of_this_set?(description) &&
              (!has_primary?(servers) || description.primary?))
        end

        # Whether a description can be used to remove hosts from the cluster.
        #
        # @example Check if a description can be used to remove hosts from the cluster.
        #   topology.remove_hosts?(description)
        #
        # @param [ Mongo::Server::Description ] description The description.
        #
        # @return [ true, false ] Whether hosts may be removed from the cluster.
        #
        # @since 2.0.6
        def remove_hosts?(description)
          !description.config.empty? &&
            (description.primary? ||
              description.me_mismatch? ||
                description.hosts.empty? ||
                  !member_of_this_set?(description))
        end

        # Whether a specific server in the cluster can be removed, given a description.
        #
        # @example Check if a specific server can be removed from the cluster.
        #   topology.remove_server?(description, server)
        #
        # @param [ Mongo::Server::Description ] description The description.
        # @param [ Mongo::Serve ] server The server in question.
        #
        # @return [ true, false ] Whether the server can be removed from the cluster.
        #
        # @since 2.0.6
        def remove_server?(description, server)
          remove_self?(description, server) ||
            (member_of_this_set?(description) && !description.lists_server?(server))
        end

        # A replica set topology is not sharded.
        #
        # @example Is the topology sharded?
        #   ReplicaSet.sharded?
        #
        # @return [ false ] Always false.
        #
        # @since 2.0.0
        def sharded?; false; end

        # A replica set topology is not single.
        #
        # @example Is the topology single?
        #   ReplicaSet.single?
        #
        # @return [ false ] Always false.
        #
        # @since 2.0.0
        def single?; false; end

        # A replica set topology is not unknown.
        #
        # @example Is the topology unknown?
        #   ReplicaSet.unknown?
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
        # @return [ Topology::ReplicaSet ] Always returns self.
        #
        # @since 2.0.6
        def standalone_discovered; self; end

        private

        def update_max_election_id(description)
          if description.election_id &&
              (@max_election_id.nil? ||
                  description.election_id > @max_election_id)
            @max_election_id = description.election_id
          end
        end

        def update_max_set_version(description)
          if description.set_version &&
              (@max_set_version.nil? ||
                  description.set_version > @max_set_version)
            @max_set_version = description.set_version
          end
        end

        def detect_stale_primary!(description)
          if description.election_id && description.set_version
            if @max_set_version && @max_election_id &&
                (description.set_version < @max_set_version ||
                    (description.set_version == @max_set_version &&
                        description.election_id < @max_election_id))
              description.unknown!
            end
          end
        end

        def has_primary?(servers)
          servers.find { |s| s.primary? }
        end

        def member_of_this_set?(description)
          description.replica_set_member? &&
            description.replica_set_name == replica_set_name
        end

        def remove_self?(description, server)
          !member_of_this_set?(description) &&
            description.is_server?(server) &&
              !description.ghost?
        end
      end
    end
  end
end
