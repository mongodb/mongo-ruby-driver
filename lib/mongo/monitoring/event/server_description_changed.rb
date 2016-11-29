# Copyright (C) 2016 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  class Monitoring
    module Event

      # Event fired when a server's description changes.
      #
      # @since 2.4.0
      class ServerDescriptionChanged

        # @return [ Address ] address The server address.
        attr_reader :address

        # @return [ Topology ] topology The topology.
        attr_reader :topology

        # @return [ Server::Description ] previous_description The previous server
        #   description.
        attr_reader :previous_description

        # @return [ Server::Description ] new_description The new server
        #   description.
        attr_reader :new_description

        # Create the event.
        #
        # @example Create the event.
        #   ServerDescriptionChanged.new(address, topology, previous, new)
        #
        # @param [ Address ] address The server address.
        # @param [ Integer ] topology The topology.
        # @param [ Server::Description ] previous_description The previous description.
        # @param [ Server::Description ] new_description The new description.
        #
        # @since 2.4.0
        def initialize(address, topology, previous_description, new_description)
          @address = address
          @topology = topology
          @previous_description = previous_description
          @new_description = new_description
        end
      end
    end
  end
end
