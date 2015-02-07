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

module Mongo

  # Base error class for all Mongo related errors.
  #
  # @since 2.0.0
  class Error < StandardError; end

  # Base class for socket errors.
  #
  # @since 2.0.0
  class SocketError < Error; end

  # Raised when a socket connection times out.
  #
  # @since 2.0.0
  class SocketTimeoutError < SocketError; end

  # Raised when a connection failure occurs.
  #
  # @since 2.0.0
  class ConnectionError < Error; end
end

# Require all the driver specific errors.
require 'mongo/error/driver_error'
require 'mongo/error/max_bson_size'
require 'mongo/error/max_message_size'
require 'mongo/error/empty_batch'
require 'mongo/error/invalid_bulk_operation'
require 'mongo/error/invalid_collection_name'
require 'mongo/error/invalid_database_name'
require 'mongo/error/invalid_document'
require 'mongo/error/invalid_file'
require 'mongo/error/invalid_replacement_document'
require 'mongo/error/invalid_update_document'
require 'mongo/error/need_primary_server'
require 'mongo/error/unsupported_features'

# Require all the operation failures.
require 'mongo/error/operation_failure'
require 'mongo/error/bulk_write_failure'
require 'mongo/error/invalid_nonce'
require 'mongo/error/invalid_signature'
