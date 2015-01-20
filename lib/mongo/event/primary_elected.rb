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

    # This handles primary elected events for server descriptions.
    #
    # @since 2.0.0
    class PrimaryElected

      # @return [ Mongo::Cluster ] cluster The event publisher.
      attr_reader :cluster

      # Initialize the new primary elected event handler.
      #
      # @example Create the new handler.
      #   PrimaryElected.new(cluster)
      #
      # @param [ Mongo::Cluster ] cluster The cluster to publish from.
      #
      # @since 2.0.0
      def initialize(cluster)
        @cluster = cluster
      end

      # This event tells the cluster to take all previous primaries to an
      # unknown state.
      #
      # @example Handle the event.
      #   primary_elected.handle(description)
      #
      # @param [ Server::Description ] description The description of the
      #   elected server.
      #
      # @since 2.0.0
      def handle(description)
        cluster.elect_primary!(description)
      end
    end
  end
end
