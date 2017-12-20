# Copyright (C) 2014-2017 MongoDB, Inc.
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

require 'mongo/operation/result'
require 'mongo/operation/executable'
require 'mongo/operation/specifiable'
require 'mongo/operation/limited'
require 'mongo/operation/object_id_generator'
require 'mongo/operation/uses_command_op_msg'
require 'mongo/operation/causally_consistent'
require 'mongo/operation/read_preference'
require 'mongo/operation/takes_write_concern'
require 'mongo/operation/read'
require 'mongo/operation/write'
require 'mongo/operation/commands'
require 'mongo/operation/kill_cursors'

module Mongo
  module Operation

    # The q field constant.
    #
    # @since 2.1.0
    Q = 'q'.freeze

    # The u field constant.
    #
    # @since 2.1.0
    U = 'u'.freeze

    # The limit field constant.
    #
    # @since 2.1.0
    LIMIT = 'limit'.freeze

    # The multi field constant.
    #
    # @since 2.1.0
    MULTI = 'multi'.freeze

    # The upsert field constant.
    #
    # @since 2.1.0
    UPSERT = 'upsert'.freeze

    # The collation field constant.
    #
    # @since 2.4.0
    COLLATION = 'collation'.freeze

    # The array filters field constant.
    #
    # @since 2.5.0
    ARRAY_FILTERS = 'arrayFilters'.freeze

    # The operation time field constant.
    #
    # @since 2.5.0
    OPERATION_TIME = 'operationTime'.freeze

    # The cluster time field constant.
    #
    # @since 2.5.0
    CLUSTER_TIME = '$clusterTime'.freeze
  end
end
