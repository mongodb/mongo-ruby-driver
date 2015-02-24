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
            log_debug([ "Server #{description.address.to_s} elected as primary in #{replica_set_name}." ])
            servers.each do |server|
              if server.primary? && server.address != description.address
                server.description.unknown!
              end
            end
          else
            log_warn([
              "Server #{description.address.to_s} in incorrect replica set: #{description.replica_set_name}."
            ])
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
        def initialize(options)
          @options = options
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

        # A replica set topology is not sharded.
        #
        # @example Is the topology sharded?
        #   ReplicaSet.sharded?
        #
        # @return [ false ] Always false.
        #
        # @since 2.0.0
        def sharded?; false; end

        # A replica set topology is not standalone.
        #
        # @example Is the topology standalone?
        #   ReplicaSet.standalone?
        #
        # @return [ false ] Always false.
        #
        # @since 2.0.0
        def standalone?; false; end

        # A replica set topology is not unknown.
        #
        # @example Is the topology unknown?
        #   ReplicaSet.unknown?
        #
        # @return [ false ] Always false.
        #
        # @since 2.0.0
        def unknown?; false; end
      end
    end
  end
end
