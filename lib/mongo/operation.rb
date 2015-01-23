# Copyright (C) 2014-2015 MongoDB, Inc.
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
require 'mongo/operation/specifiable'
require 'mongo/operation/limited'
require 'mongo/operation/batchable'
require 'mongo/operation/read'
require 'mongo/operation/write'
require 'mongo/operation/aggregate'
require 'mongo/operation/command'
require 'mongo/operation/kill_cursors'
require 'mongo/operation/map_reduce'

module Mongo
  module Operation

    # Legacy error message field.
    #
    # @since 2.0.0
    ERROR = 'err'.freeze

    # The write errors field in the response, 2.6 and higher.
    #
    # @since 2.0.0
    WRITE_ERRORS = 'writeErrors'.freeze

    # Constant for the error code field.
    #
    # @since 2.0.0
    ERROR_CODE = 'code'.freeze

    # Constant for the error message field.
    #
    # @since 2.0.0
    ERROR_MSG = 'errmsg'.freeze

    # Constant for an unknown error.
    #
    # @since 2.0.0
    UNKNOWN_ERROR = 8.freeze

    # Constant for a bad value error.
    #
    # @since 2.0.0
    BAD_VALUE = 2.freeze
  end
end
