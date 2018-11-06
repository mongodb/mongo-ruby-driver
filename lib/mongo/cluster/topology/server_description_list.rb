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
      # A list of server descriptions. Stored in a topology.
      #
      # This class adds methods for retrieving server descriptions
      # by their address.
      #
      # @since 2.7.0
      class ServerDescriptionList < Array
        # Returns a server description for the given address, or nil
        # if there isn't one.
        #
        # @return [ Server::Description | nil ] Server description.
        #
        # @since 2.7.0
        def for_address(address)
          detect do |desc|
            if address.is_a?(::Mongo::Address)
              desc.address == address
            else
              desc.address.to_s == address
            end
          end
        end

        # Returns a server description for the given address.
        # If there isn't one, raises KeyError.
        #
        # @return [ Server::Description ] Server description.
        #
        # @since 2.7.0
        def for_address!(address)
          unless rv = for_address(address)
            raise KeyError, "#{address} not in server descriptions"
          end
          rv
        end
      end
    end
  end
end
