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

module Mongo
  class Error

    # Raised when the driver does not support the complete set of server
    # features.
    #
    # @since 2.0.0
    class UnsupportedFeatures < Error

      # Initialize the exception.
      #
      # @example Initialize the exception.
      #   Unsupported.new(0..3)
      #
      # @param [ Range ] server_wire_versions The server's supported wire
      #   versions.
      #
      # @since 2.0.0
      def initialize(server_wire_versions)
        super(
          "This version of the driver, #{Mongo::VERSION}, only supports wire " +
          "protocol versions #{Server::Description::Features::DRIVER_WIRE_VERSIONS} " +
          "and the server supports wire versions #{server_wire_versions}. " + 
          "Please upgrade the driver to be able to support this server version."
        )
      end
    end
  end
end
