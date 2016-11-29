# Copyright (C) 2015 MongoDB, Inc.
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
  module Event

    # This handles when a standalone is discovered.
    #
    # @since 2.0.6
    class StandaloneDiscovered

      # @return [ Mongo::Cluster ] cluster The cluster.
      attr_reader :cluster

      # Initialize the new standalone discovered event handler.
      #
      # @example Create the new handler.
      #   StandaloneDiscovered.new(cluster)
      #
      # @param [ Mongo::Cluster ] cluster The cluster to publish from.
      #
      # @since 2.0.6
      def initialize(cluster)
        @cluster = cluster
      end

      # This event tells the cluster to notify its topology that a standalone
      # was discovered.
      #
      # @example Handle the event.
      #   standalone_discovered.handle(description)
      #
      # @param [ Server::Description ] description The description of the
      #   server.
      #
      # @since 2.0.6
      def handle(description)
        cluster.standalone_discovered
      end
    end
  end
end
