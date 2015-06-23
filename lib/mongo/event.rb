# Copyright (C) 2014-2015 MongoDB, Inc.
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

require 'mongo/event/listeners'
require 'mongo/event/publisher'
require 'mongo/event/subscriber'
require 'mongo/event/primary_elected'
require 'mongo/event/description_changed'
require 'mongo/event/standalone_discovered'

module Mongo
  module Event

    # When a standalone is discovered.
    #
    # @since 2.0.6
    STANDALONE_DISCOVERED = 'standalone_discovered'.freeze

    # When a server is elected primary.
    #
    # @since 2.0.0
    PRIMARY_ELECTED = 'primary_elected'.freeze

    # When a server is to be removed from a cluster.
    #
    # @since 2.0.6
    DESCRIPTION_CHANGED = 'description_changed'.freeze
  end
end
