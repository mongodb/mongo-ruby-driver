# Copyright (C) 2018 MongoDB, Inc.
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

      # Defines behavior common to all topologies.
      #
      # @since 2.7.0
      class Base
        extend Forwardable

        # Initialize the topology with the options.
        #
        # @param [ Hash ] options The options.
        # @param [ Monitoring ] monitoring The monitoring.
        # @param [ Cluster ] cluster The cluster.
        #
        # @since 2.7.0
        # @api private
        def initialize(options, monitoring, cluster)
          @options = options
          @monitoring = monitoring
          @cluster = cluster
        end

        # @return [ Hash ] options The options.
        attr_reader :options

        # @return [ Cluster ] The cluster.
        # @api private
        attr_reader :cluster
        private :cluster

        # @return [ Array<String> ] addresses Server addresses.
        def addresses
          cluster.addresses.map(&:seed)
        end

        # @return [ monitoring ] monitoring the monitoring.
        attr_reader :monitoring

        # Notify the topology that a standalone was discovered.
        #
        # @example Notify the topology that a standalone was discovered.
        #   topology.standalone_discovered
        #
        # @return [ Topology::ReplicaSet ] Always returns self.
        #
        # @since 2.0.6
        # @deprecated Does nothing and will be removed in version 3.0.0
        def standalone_discovered; self; end
      end
    end
  end
end
