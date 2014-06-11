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

# Base Operations
require 'mongo/operation/executable'
require 'mongo/operation/read'
require 'mongo/operation/write'

# special logic - sometimes read, sometimes write, neither
require 'mongo/operation/aggregate'
require 'mongo/operation/map_reduce'
require 'mongo/operation/command'
require 'mongo/operation/kill_cursors'

module Mongo

  module Operation

    # The name of the virtual collection to which the command 'query' is
    # sent.
    #
    # @return [ String ] The command collection.
    #
    # @since 3.0.0
    COMMAND_COLLECTION_NAME = '$cmd'

    # The default server preference for an operation.
    #
    # @return [ Mongo::ServerPreference::Primary ] A primary server preference.
    #
    # @since 3.0.0
    DEFAULT_SERVER_PREFERENCE = Mongo::ServerPreference.get(:primary)
  end
end


