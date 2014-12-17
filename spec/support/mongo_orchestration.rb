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

require 'support/mongo_orchestration/requestable'
require 'support/mongo_orchestration/standalone'

module MongoOrchestration
  extend self

  TYPES = {
      #replica_set: ReplicaSet,
      #sharded_cluster: ShardedCluster,
      standalone: Standalone
    }

  # The default base uri for mongo orchestration.
  #
  # @since 2.0.0
  DEFAULT_BASE_URI = 'http://localhost:8889'.freeze

  def get(type, options = {})
    TYPES[type].new(options)
  end

  class ServiceNotAvailable < RuntimeError

    def initialize
      super("The Mongo Orchestration service is not available.")
    end
  end
end