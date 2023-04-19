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

require 'mongo/address/ipv4'
require 'mongo/address/ipv6'
require 'mongo/address/unix'
require 'mongo/address/validator'

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
    # @option options [ Float ] :connect_timeout Connect timeout.
    #
    # @since 2.0.0
    def initialize(seed, options = {})
      if seed.nil?
        raise ArgumentError, "address must be not nil"
      end
      @seed = seed
      @host, @port = parse_host_port
      @options = Hash[options.map { |k, v| [k.to_sym, v] }]
    end

    # @return [ String ] seed The seed address.
    attr_reader :seed

    # @return [ String ] host The original host name.
    attr_reader :host

    # @return [ Integer ] port The port.
    attr_reader :port

    # @api private
    attr_reader :options

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

    # Get a socket for the address stored in this object, given the options.
    #
    # If the address stored in this object looks like a Unix path, this method
    # returns a Unix domain socket for this path.
    #
    # Otherwise, this method attempts to resolve the address stored in
    # this object to IPv4 and IPv6 addresses using +Socket#getaddrinfo+, then
    # connects to the resulting addresses and returns the socket of the first
    # successful connection. The order in which address families (IPv4/IPV6)
    # are tried is the same order in which the addresses are returned by
    # +getaddrinfo+, and is determined by the host system.
    #
    # Name resolution is performed on each +socket+ call. This is done so that
    # any changes to which addresses the host names used as seeds or in
    # server configuration resolve to are immediately noticed by the driver,
    # even if a socket has been connected to the affected host name/address
    # before. However, note that DNS TTL values may still affect when a change
    # to a host address is noticed by the driver.
    #
    # This method propagates any exceptions raised during DNS resolution and
    # subsequent connection attempts. In case of a host name resolving to
    # multiple IP addresses, the error raised by the last attempt is propagated
    # to the caller. This method does not map exceptions to Mongo::Error
    # subclasses, and may raise any subclass of Exception.
    #
    # @example Get a socket.
    #   address.socket(5, :ssl => true)
    #
    # @param [ Float ] socket_timeout The socket timeout.
    # @param [ Hash ] opts The options.
    #
    # @option opts [ Float ] :connect_timeout Connect timeout.
    # @option opts [ true | false ] :ssl Whether to use SSL.
    # @option opts [ String ] :ssl_ca_cert
    #   Same as the corresponding Client/Socket::SSL option.
    # @option opts [ Array<OpenSSL::X509::Certificate> ] :ssl_ca_cert_object
    #   Same as the corresponding Client/Socket::SSL option.
    # @option opts [ String ] :ssl_ca_cert_string
    #   Same as the corresponding Client/Socket::SSL option.
    # @option opts [ String ] :ssl_cert
    #   Same as the corresponding Client/Socket::SSL option.
    # @option opts [ OpenSSL::X509::Certificate ] :ssl_cert_object
    #   Same as the corresponding Client/Socket::SSL option.
    # @option opts [ String ] :ssl_cert_string
    #   Same as the corresponding Client/Socket::SSL option.
    # @option opts [ String ] :ssl_key
    #   Same as the corresponding Client/Socket::SSL option.
    # @option opts [ OpenSSL::PKey ] :ssl_key_object
    #   Same as the corresponding Client/Socket::SSL option.
    # @option opts [ String ] :ssl_key_pass_phrase
    #   Same as the corresponding Client/Socket::SSL option.
    # @option opts [ String ] :ssl_key_string
    #   Same as the corresponding Client/Socket::SSL option.
    # @option opts [ true, false ] :ssl_verify
    #   Same as the corresponding Client/Socket::SSL option.
    # @option opts [ true, false ] :ssl_verify_certificate
    #   Same as the corresponding Client/Socket::SSL option.
    # @option opts [ true, false ] :ssl_verify_hostname
    #   Same as the corresponding Client/Socket::SSL option.
    #
    # @return [ Mongo::Socket::SSL | Mongo::Socket::TCP | Mongo::Socket::Unix ]
    #   The socket.
    #
    # @raise [ Mongo::Error ] If network connection failed.
    #
    # @since 2.0.0
    # @api private
    def socket(socket_timeout, opts = {})
      opts = {
        connect_timeout: Server::CONNECT_TIMEOUT,
      }.update(options).update(Hash[opts.map { |k, v| [k.to_sym, v] }])

      map_exceptions do
        if seed.downcase =~ Unix::MATCH
          specific_address = Unix.new(seed.downcase)
          return specific_address.socket(socket_timeout, opts)
        end

        # When the driver connects to "localhost", it only attempts IPv4
        # connections. When the driver connects to other hosts, it will
        # attempt both IPv4 and IPv6 connections.
        family = (host == LOCALHOST) ? ::Socket::AF_INET : ::Socket::AF_UNSPEC
        error = nil
        # Sometimes Socket#getaddrinfo returns the same info more than once
        # (multiple identical items in the returned array). It does not make
        # sense to try to connect to the same address more than once, thus
        # eliminate duplicates here.
        infos = ::Socket.getaddrinfo(host, nil, family, ::Socket::SOCK_STREAM)
        results = infos.map do |info|
          [info[4], info[3]]
        end.uniq
        results.each do |family, address_str|
          begin
            specific_address = FAMILY_MAP[family].new(address_str, port, host)
            socket = specific_address.socket(socket_timeout, opts)
            return socket
          rescue IOError, SystemCallError, Error::SocketTimeoutError, Error::SocketError => e
            error = e
          end
        end
        raise error
      end
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
      if port
        if host.include?(':')
          "[#{host}]:#{port}"
        else
          "#{host}:#{port}"
        end
      else
        host
      end
    end

    private

    def parse_host_port
      address = seed.downcase
      case address
        when Unix::MATCH then Unix.parse(address)
        when IPv6::MATCH then IPv6.parse(address)
        else IPv4.parse(address)
      end
    end

    def map_exceptions
      begin
        yield
      rescue Errno::ETIMEDOUT => e
        raise Error::SocketTimeoutError, "#{e.class}: #{e} (for #{self})"
      rescue IOError, SystemCallError => e
        raise Error::SocketError, "#{e.class}: #{e} (for #{self})"
      rescue OpenSSL::SSL::SSLError => e
        raise Error::SocketError, "#{e.class}: #{e} (for #{self})"
      end
    end
  end
end
