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

  # Base error class for all errors coming from the server.
  #
  # @since 2.0.0
  class OperationError < Error; end

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

require 'mongo/error/driver_error'
require 'mongo/error/need_primary_server'
