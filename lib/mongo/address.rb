# Copyright (C) 2014-2017 MongoDB, Inc.
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

require 'mongo/address/ipv4'
require 'mongo/address/ipv6'
require 'mongo/address/unix'

module Mongo

  # Represents an address to a server, either with an IP address or socket
  # path.
  #
  # @since 2.0.0
  class Address
    extend Forwardable

    # Mapping from socket family to resolver class.
    #
    # @since 2.0.0
    FAMILY_MAP = {
      ::Socket::PF_UNIX => Unix,
      ::Socket::AF_INET6 => IPv6,
      ::Socket::AF_INET => IPv4
    }.freeze

    # The localhost constant.
    #
    # @since 2.1.0
    LOCALHOST = 'localhost'.freeze

    # @return [ String ] seed The seed address.
    attr_reader :seed

    # @return [ String ] host The original host name.
    attr_reader :host

    # @return [ Integer ] port The port.
    attr_reader :port

    # Check equality of the address to another.
    #
    # @example Check address equality.
    #   address == other
    #
    # @param [ Object ] other The other object.
    #
    # @return [ true, false ] If the objects are equal.
    #
    # @since 2.0.0
    def ==(other)
      return false unless other.is_a?(Address)
      host == other.host && port == other.port
    end

    # Check equality for hashing.
    #
    # @example Check hashing equality.
    #   address.eql?(other)
    #
    # @param [ Object ] other The other object.
    #
    # @return [ true, false ] If the objects are equal.
    #
    # @since 2.2.0
    def eql?(other)
      self == other
    end

    # Calculate the hash value for the address.
    #
    # @example Calculate the hash value.
    #   address.hash
    #
    # @return [ Integer ] The hash value.
    #
    # @since 2.0.0
    def hash
      [ host, port ].hash
    end

    # Initialize the address.
    #
    # @example Initialize the address with a DNS entry and port.
    #   Mongo::Address.new("app.example.com:27017")
    #
    # @example Initialize the address with a DNS entry and no port.
    #   Mongo::Address.new("app.example.com")
    #
    # @example Initialize the address with an IPV4 address and port.
    #   Mongo::Address.new("127.0.0.1:27017")
    #
    # @example Initialize the address with an IPV4 address and no port.
    #   Mongo::Address.new("127.0.0.1")
    #
    # @example Initialize the address with an IPV6 address and port.
    #   Mongo::Address.new("[::1]:27017")
    #
    # @example Initialize the address with an IPV6 address and no port.
    #   Mongo::Address.new("[::1]")
    #
    # @example Initialize the address with a unix socket.
    #   Mongo::Address.new("/path/to/socket.sock")
    #
    # @param [ String ] seed The provided address.
    # @param [ Hash ] options The address options.
    #
    # @since 2.0.0
    def initialize(seed, options = {})
      @seed = seed
      @host, @port = parse_host_port
      @options = options
    end

    # Get a pretty printed address inspection.
    #
    # @example Get the address inspection.
    #   address.inspect
    #
    # @return [ String ] The nice inspection string.
    #
    # @since 2.0.0
    def inspect
      "#<Mongo::Address:0x#{object_id} address=#{to_s}>"
    end

    # Get a socket for the provided address, given the options.
    #
    # @example Get a socket.
    #   address.socket(5, :ssl => true)
    #
    # @param [ Float ] socket_timeout The socket timeout.
    # @param [ Hash ] ssl_options SSL options.
    #
    # @return [ Pool::Socket::SSL, Pool::Socket::TCP, Pool::Socket::Unix ] The socket.
    #
    # @since 2.0.0
    def socket(socket_timeout, ssl_options = {})
      @resolver ||= initialize_resolver!(ssl_options)
      @resolver.socket(socket_timeout, ssl_options)
    end

    # Get the address as a string.
    #
    # @example Get the address as a string.
    #   address.to_s
    #
    # @return [ String ] The nice string.
    #
    # @since 2.0.0
    def to_s
      port ? "#{host}:#{port}" : host
    end

    # Connect a socket.
    #
    # @example Connect a socket.
    #   address.connect_socket!(socket)
    #
    # @since 2.4.3
    def connect_socket!(socket)
      socket.connect!(connect_timeout)
    end

    private

    def connect_timeout
      @connect_timeout ||= @options[:connect_timeout] || Server::CONNECT_TIMEOUT
    end

    def initialize_resolver!(ssl_options)
      return Unix.new(seed.downcase) if seed.downcase =~ Unix::MATCH

      family = (host == LOCALHOST) ? ::Socket::AF_INET : ::Socket::AF_UNSPEC
      error = nil
      ::Socket.getaddrinfo(host, nil, family, ::Socket::SOCK_STREAM).each do |info|
        begin
          res = FAMILY_MAP[info[4]].new(info[3], port, host)
          res.socket(connect_timeout, ssl_options).connect!(connect_timeout).close
          return res
        rescue IOError, SystemCallError, Error::SocketTimeoutError, Error::SocketError => e
          error = e
        end
      end
      raise error
    end

    def parse_host_port
      address = seed.downcase
      case address
        when Unix::MATCH then Unix.parse(address)
        when IPv6::MATCH then IPv6.parse(address)
        else IPv4.parse(address)
      end
    end
  end
end
