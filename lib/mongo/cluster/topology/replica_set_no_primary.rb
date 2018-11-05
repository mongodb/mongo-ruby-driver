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
  class Cluster
    module Topology

      # Defines behavior when a cluster is in replica set topology,
      # and there is no primary or the primary has not yet been discovered
      # by the driver.
      #
      # @since 2.0.0
      class ReplicaSetNoPrimary < Base
        include Loggable
        include Monitoring::Publishable

        # Initialize the topology with the options.
        #
        # @param [ Hash ] options The options.
        # @param [ Monitoring ] monitoring The monitoring.
        # @param [ Cluster ] cluster The cluster.
        # @param max_election_id For internal driver use only.
        # @param max_set_version For internal driver use only.
        #
        # @option options [ Symbol ] :replica_set Name of the replica set to
        #   connect to. Can be left blank (either nil or the empty string are
        #   accepted) to discover the name from the cluster. If the addresses
        #   belong to different replica sets there is no guarantee which
        #   replica set is selected - in particular, the driver may choose
        #   the replica set name of a secondary if it returns its response
        #   prior to a primary belonging to a different replica set.
        #
        # @since 2.7.0
        # @api private
        def initialize(options, monitoring, cluster,
          max_election_id = nil, max_set_version = nil
        )
          super(options, monitoring, cluster)

          unless replica_set_name
            raise ArgumentError, 'Cannot instantiate a replica set topology without a replica set name'
          end

          @max_election_id = max_election_id
          @max_set_version = max_set_version
        end

        # The display name for the topology.
        #
        # @since 2.0.0
        # @deprecated
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
          self.class.name.gsub(/.*::/, '')
        end

        # @note This method is experimental and subject to change.
        #
        # @api experimental
        # @since 2.7.0
        def summary
          "#{display_name.gsub(' ', '')}[name=#{replica_set_name},v=#{@max_set_version},e=#{@max_election_id && @max_election_id.to_s.sub(/^0+/, '')}]"
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
        # @return [ ReplicaSetWithPrimary ] The topology.
        def elect_primary(description, servers)
          self
        end

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
        # @return [ true, false ] If a readable server is present.
        #
        # @since 2.4.0
        def has_readable_server?(cluster, server_selector = nil)
          (server_selector || ServerSelector.get(mode: :primary)).candidates(cluster).any?
        end

        # Determine if the topology would select a writable server for the
        # provided candidates.
        #
        # @example Is a writable server present?
        #   topology.has_writable_server?(servers)
        #
        # @param [ Cluster ] cluster The cluster.
        #
        # @return [ true, false ] If a writable server is present.
        #
        # @since 2.4.0
        def has_writable_server?(cluster)
          cluster.servers.any?{ |server| server.primary? }
        end

        # A replica set topology is a replica set.
        #
        # @example Is the topology a replica set?
        #   topology.replica_set?
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
          @replica_set_name ||= begin
            v = options[:replica_set_name]
            if v == ''
              v = nil
            end
            v
          end
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

        # Notify the topology that a member was discovered.
        #
        # @example Notify the topology that a member was discovered.
        #   topology.member_discovered
        #
        # @since 2.4.0
        def member_discovered; end;

        # The largest electionId ever reported by a primary.
        # May be nil.
        #
        # @return [ BSON::ObjectId ] The election id.
        #
        # @since 2.7.0
        attr_reader :max_election_id

        # The largest setVersion ever reported by a primary.
        # May be nil.
        #
        # @return [ Integer ] The set version.
        #
        # @since 2.7.0
        attr_reader :max_set_version

        # @api private
        def new_max_election_id(description)
          if description.election_id &&
              (@max_election_id.nil? ||
                  description.election_id > @max_election_id)
            description.election_id
          else
            @max_election_id
          end
        end

        # @api private
        def new_max_set_version(description)
          if description.set_version &&
              (@max_set_version.nil? ||
                  description.set_version > @max_set_version)
            description.set_version
          else
            @max_set_version
          end
        end

        private

        def has_primary?(servers)
          servers.find { |s| s.primary? }
        end

        def member_of_this_set?(description)
          description.replica_set_member? &&
            description.replica_set_name == replica_set_name
        end
      end
    end
  end
end
