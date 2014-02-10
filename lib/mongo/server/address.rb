# Copyright (C) 2009-2014 MongoDB, Inc.
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

require 'resolv'

module Mongo
  class Server

    # Represents an address to a server, either with an IP address or socket
    # path.
    #
    # @since 3.0.0
    class Address

      # @return [ String ] host The original host provided.
      attr_reader :host

      # @return [ String ] ip The resolved ip address.
      attr_reader :ip

      # @return [ Integer ] port The port to the connect to.
      attr_reader :port

      # Initialize the address.
      #
      # @example Initialize the address with a DNS entry and port.
      #   Mongo::Server::Address.new("app.example.com:27017")
      #
      # @example Initialize the address with a DNS entry and no port.
      #   Mongo::Server::Address.new("app.example.com")
      #
      # @example Initialize the address with an IPV4 address and port.
      #   Mongo::Server::Address.new("127.0.0.1:27017")
      #
      # @example Initialize the address with an IPV4 address and no port.
      #   Mongo::Server::Address.new("127.0.0.1")
      #
      # @example Initialize the address with a unix socket.
      #   Mongo::Server::Address.new("/path/to/socket.sock")
      #
      # @param [ String ] address The provided address, ip or DNS entry.
      # @param [ Hash ] options The address options.
      #
      # @since 3.0.0
      def initialize(address, options = {})
        split = address.split(':')
        @host = split[0]
        unless @host =~ /\.sock/
          @port = (split[1] || 27017).to_i
          resolve!
        end
      end

      # Resolve the DNS to an ip address. Will mutate this object if the DNS
      # changed.
      #
      # @example Resolve the DNS for the address.
      #   address.resolve!
      #
      # @return [ String ] The resolved ip address.
      #
      # @since 3.0.0
      def resolve!
        @ip = Resolv.getaddress(host)
      end
    end
  end
end
