# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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

module Mongo
  module Event

    # When a standalone is discovered.
    #
    # @since 2.0.6
    # @deprecated Will be removed in 3.0
    STANDALONE_DISCOVERED = 'standalone_discovered'.freeze

    # When a server is elected primary.
    #
    # @since 2.0.0
    # @deprecated Will be removed in 3.0
    PRIMARY_ELECTED = 'primary_elected'.freeze

    # When a server is discovered to be a member of a topology.
    #
    # @since 2.4.0
    # @deprecated Will be removed in 3.0
    MEMBER_DISCOVERED = 'member_discovered'.freeze

    # When a server is to be removed from a cluster.
    #
    # @since 2.0.6
    # @deprecated Will be removed in 3.0
    DESCRIPTION_CHANGED = 'description_changed'.freeze
  end
end

require 'mongo/event/base'
require 'mongo/event/listeners'
require 'mongo/event/publisher'
require 'mongo/event/subscriber'
