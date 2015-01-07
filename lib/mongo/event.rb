# Copyright (C) 2009-2014 MongoDB, Inc.
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
require 'mongo/event/server_added'
require 'mongo/event/server_removed'

module Mongo
  module Event

    # When a server is to be added to a cluster.
    #
    # @since 2.0.0
    SERVER_ADDED = 'server_added'.freeze

    # When a server is to be removed from a cluster.
    #
    # @since 2.0.0
    SERVER_REMOVED = 'server_removed'.freeze
  end
end
