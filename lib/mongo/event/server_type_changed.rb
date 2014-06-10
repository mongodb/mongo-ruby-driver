
# Copyright (C) 2009-2014 MongoDB, Inc.
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

    # This handles server type changed events.
    #
    # @since 2.0.0
    class ServerTypeChanged
      include Loggable

      # @return [ Mongo::Cluster ] The listening cluster.
      attr_reader :cluster

      # Initialize the new server changed event handler.
      #
      # @example Create the new handler.
      #   ServerTypeChanged.new(cluster)
      #
      # @param [ Mongo::Cluster ] cluster The cluster to publish from.
      #
      # @since 2.0.0
      def initialize(cluster)
        @cluster = cluster
      end

      # This event publishes an event to the cluster and the cluster will
      # decide based on rules what to do.
      #
      # @example Handle the event.
      #   server_type_changed.handle(new_type)
      #
      # @param [ Symbol ] old_type The old server type.
      # @param [ Symbol ] new_type The new server type.
      #
      # @since 2.0.0
      def handle(old_type, new_type)
        log(:debug, 'MONGODB', [ "Server changed from #{old_type} to #{new_type}." ])
      end
    end
  end
end
