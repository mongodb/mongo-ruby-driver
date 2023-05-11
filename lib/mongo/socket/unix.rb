# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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
  class Socket

    # Wrapper for Unix sockets.
    #
    # @since 2.0.0
    class Unix < Socket

      # Initializes a new Unix socket.
      #
      # @example Create the Unix socket.
      #   Unix.new('/path/to.sock', 5)
      #
      # @param [ String ] path The path.
      # @param [ Float ] timeout The socket timeout value.
      # @param [ Hash ] options The options.
      #
      # @option options [ Float ] :connect_timeout Connect timeout (unused).
      # @option options [ Address ] :connection_address Address of the
      #   connection that created this socket.
      # @option options [ Integer ] :connection_generation Generation of the
      #   connection (for non-monitoring connections) that created this socket.
      # @option options [ true | false ] :monitor Whether this socket was
      #   created by a monitoring connection.
      #
      # @since 2.0.0
      # @api private
      def initialize(path, timeout, options = {})
        super(timeout, options)
        @path = path
        @socket = ::UNIXSocket.new(path)
        set_socket_options(@socket)
      end

      # @return [ String ] path The path to connect to.
      attr_reader :path

      private

      def human_address
        path
      end
    end
  end
end

