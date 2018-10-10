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

        # Initialize the topology with the options.
        #
        # @param [ Hash ] options The options.
        # @param [ Monitoring ] monitoring The monitoring.
        # @param [ Array<String> ] addresses Addresses of servers in the topology.
        #
        # @since 2.7.0
        # @api private
        def initialize(options, monitoring, addresses = [])
          @options = options
          @monitoring = monitoring
          @addresses = addresses
        end

        # @return [ Hash ] options The options.
        attr_reader :options

        # @return [ Array<String> ] addresses Server addresses.
        attr_reader :addresses

        # @return [ monitoring ] monitoring the monitoring.
        attr_reader :monitoring
      end
    end
  end
end
