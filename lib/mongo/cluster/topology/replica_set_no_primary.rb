# Copyright (C) 2014-2020 MongoDB Inc.
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
          details = server_descriptions.keys.join(',')
          if details != ''
            details << ','
          end
          details << "name=#{replica_set_name}"
          if max_set_version
            details << ",v=#{max_set_version}"
          end
          if max_election_id
            details << ",e=#{max_election_id && max_election_id.to_s.sub(/^0+/, '')}"
          end
          "#{display_name}[#{details}]"
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
        # @deprecated
        def has_readable_server?(cluster, server_selector = nil)
          !(server_selector || ServerSelector.primary).try_select_server(cluster).nil?
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

        private

        def validate_options(options, cluster)
          if options[:replica_set_name] == ''
            options = options.merge(replica_set_name: nil)
          end

          unless options[:replica_set_name]
            raise ArgumentError, 'Cannot instantiate a replica set topology without a replica set name'
          end

          super(options, cluster)
        end
      end
    end
  end
end
