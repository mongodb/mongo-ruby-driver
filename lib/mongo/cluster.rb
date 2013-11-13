# Copyright (C) 2009-2013 MongoDB, Inc.
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

    # @return [ Array<String> ] The provided seed addresses.
    attr_reader :addresses
    # @return [ Hash ] The options hash.
    attr_reader :options

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
      addresses == other.addresses
    end

    # Add a node to the cluster with the provided address. Useful in
    # auto-discovery of new nodes when an existing node executes an ismaster
    # and potentially non-configured nodes were included.
    #
    # @example Add the node for the address to the cluster.
    #   cluster.add('127.0.0.1:27018')
    #
    # @param [ String ] address The address of the node to add.
    #
    # @return [ Node ] The newly added node, if not present already.
    #
    # @since 2.0.0
    def add(address)
      unless addresses.include?(address)
        node = Node.new(self, address, options)
        addresses.push(address)
        @nodes.push(node)
        node
      end
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
      @options = options
      @nodes = addresses.map { |address| Node.new(self, address, options) }
    end

    # Get a list of node candidates from the cluster that can have operations
    # executed on them.
    #
    # @example Get the node candidates for an operation.
    #   cluster.nodes
    #
    # @return [ Array<Node> ] The candidate nodes.
    #
    # @since 2.0.0
    def nodes
      @nodes.select { |node| node.operable? }
    end
  end
end
