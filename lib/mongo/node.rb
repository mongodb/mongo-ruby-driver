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

  # Model for a MongoDB node (replica set member or single instance)
  class Node

    attr_reader :address, :cluster, :options

    def ==(other)
      address == other.address
    end

    # @todo This should be synchronized. I envison this checks if the node is
    # alive and a primary or secondary. (no arbiters)
    def operable?
      true
    end

    def initialize(cluster, address, options = {})
      @cluster = cluster
      @address = address
      @options = options
    end
  end
end
