# Copyright (C) 2013 MongoDB, Inc.
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

  # Represents a group of nodes on the server side, either as a single node, a
  # replica set, or a single or multiple mongos.
  #
  # @since 2.0.0
  class Cluster

    attr_reader :addresses, :nodes

    # Determine if this cluster of nodes is equal to another object. Checks the
    # nodes currently in the cluster, not what was configured.
    #
    # @example Is the cluster equal to the object?
    #   cluster == other
    #
    # @param [ Object ] other The object to compare to.
    #
    # @return [ true, false ] If the objects are equal.
    #
    # @since 2.0.0
    def ==(other)
      return false unless other.is_a?(Cluster)
      nodes == other.nodes
    end

    # Instantiate the new cluster.
    #
    # @example Instantiate the cluster.
    #   Mongo::Cluster.new(["127.0.0.1:27017"])
    #
    # @param [ Array<String> ] addresses The addresses of the configured nodes.
    # @param [ Hash ] options The options.
    #
    # @since 2.0.0
    def initialize(addresses, options = {})
      @addresses = addresses
      @nodes = addresses.map { |address| Node.new(address, options) }
    end
  end
end
