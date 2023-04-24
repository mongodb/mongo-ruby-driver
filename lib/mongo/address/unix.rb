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
  class Address

    # Sets up socket addresses.
    #
    # @since 2.0.0
    class Unix

      # @return [ String ] host The host.
      attr_reader :host

      # @return [ nil ] port Will always be nil.
      attr_reader :port

      # The regular expression to use to match a socket path.
      #
      # @since 2.0.0
      MATCH = Regexp.new('\.sock').freeze

      # Parse a socket path.
      #
      # @example Parse the address.
      #   Unix.parse("/path/to/socket.sock")
      #
      # @param [ String ] address The address to parse.
      #
      # @return [ Array<String> ] A list with the host (socket path).
      #
      # @since 2.0.0
      def self.parse(address)
        [ address ]
      end

      # Initialize the socket resolver.
      #
      # @example Initialize the resolver.
      #   Unix.new("/path/to/socket.sock", "/path/to/socket.sock")
      #
      # @param [ String ] host The host.
      #
      # @since 2.0.0
      def initialize(host, port=nil, host_name=nil)
        @host = host
      end

      # Get a socket for the provided address type, given the options.
      #
      # @example Get a Unix socket.
      #   address.socket(5)
      #
      # @param [ Float ] socket_timeout The socket timeout.
      # @param [ Hash ] options The options.
      #
      # @option options [ Float ] :connect_timeout Connect timeout.
      #
      # @return [ Mongo::Socket::Unix ] The socket.
      #
      # @since 2.0.0
      # @api private
      def socket(socket_timeout, options = {})
        Socket::Unix.new(host, socket_timeout, options)
      end
    end
  end
end
