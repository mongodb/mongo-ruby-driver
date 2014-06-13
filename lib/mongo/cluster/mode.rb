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

require 'mongo/cluster/mode/replica_set'
require 'mongo/cluster/mode/sharded'
require 'mongo/cluster/mode/standalone'

module Mongo
  class Cluster

    # Defines behaviour for getting selection modes.
    #
    # @since 2.0.0
    module Mode

      # The 2 various modes for server selection.
      #
      # @since 2.0.0
      OPTIONS = {
        replica_set: ReplicaSet,
        sharded: Sharded,
        standalone: Standalone
      }

      # Get the cluster mode for the provided options.
      #
      # @example Get the cluster mode.
      #   Mode.get(mode: :replica_set)
      #
      # @param [ Hash ] options The cluster options.
      #
      # @return [ ReplicaSet, Sharded, Standalone ] The mode.
      #
      # @since 2.0.0
      def self.get(options)
        OPTIONS.fetch(options[:mode]) { ReplicaSet }
      end
    end
  end
end
