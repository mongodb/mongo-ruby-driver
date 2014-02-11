# Copyright (C) 2009-2014 MongoDB, Inc.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#  http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'mongo/pool/socket/base'
require 'mongo/pool/socket/tcp'
require 'mongo/pool/socket/ssl'
require 'mongo/pool/socket/unix'

module Mongo
  class Pool
    module Socket

      class << self

        # Factory method to create a specific type of socket based on the
        # provided options.
        #
        # @example Create a TCP socket.
        #   Socket.create('127.0.0.1', 27017, 5)
        #
        # @example Create an SSL socket.
        #   Socket.create('127.0.0.1', 27017, 5, :ssl => true)
        #
        # @example Create a Unix socket.
        #   Socket.create('/path/to/socket.sock', nil, 5)
        #
        # @param [ String ] host The host to connect to.
        # @param [ String, nil ] port The port to connect to.
        # @param [ Integer ] timeout The connection timeout.
        # @param [ Hash ] options The ssl options.
        #
        # @return [ Socket ] The socket.
        #
        # @since 3.0.0
        def create(host, port, timeout, options = {})
          if !options.empty?
            Socket::SSL.new(host, port, timeout, options)
          elsif port.nil?
            Socket::Unix.new(host, timeout)
          else
            Socket::TCP.new(host, port, timeout)
          end
        end
      end
    end
  end
end
