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

    # Sets up resolution with IPv4 support if the address is an ip
    # address.
    #
    # @since 2.0.0
    class IPv4

      # @return [ String ] host The host.
      attr_reader :host

      # @return [ String ] host_name The original host name.
      attr_reader :host_name

      # @return [ Integer ] port The port.
      attr_reader :port

      # The regular expression to use to match an IPv4 ip address.
      #
      # @since 2.0.0
      MATCH = Regexp.new('/\./').freeze

      # Split value constant.
      #
      # @since 2.1.0
      SPLIT = ':'.freeze

      # Parse an IPv4 address into its host and port.
      #
      # @example Parse the address.
      #   IPv4.parse("127.0.0.1:28011")
      #
      # @param [ String ] address The address to parse.
      #
      # @return [ Array<String, Integer> ] The host and port pair.
      #
      # @since 2.0.0
      def self.parse(address)
        parts = address.split(SPLIT)
        host = parts[0]
        port = (parts[1] || 27017).to_i
        [ host, port ]
      end

      # Initialize the IPv4 resolver.
      #
      # @example Initialize the resolver.
      #   IPv4.new("127.0.0.1", 27017, 'localhost')
      #
      # @param [ String ] host The host.
      # @param [ Integer ] port The port.
      #
      # @since 2.0.0
      def initialize(host, port, host_name=nil)
        @host = host
        @port = port
        @host_name = host_name
      end

      # Get a socket for the provided address type, given the options.
      #
      # @example Get an IPv4 socket.
      #   ipv4.socket(5, :ssl => true)
      #
      # @param [ Float ] socket_timeout The socket timeout.
      # @param [ Hash ] options The options.
      #
      # @option options [ Float ] :connect_timeout Connect timeout.
      # @option options [ true | false ] :ssl Whether to use TLS.
      # @option options [ String ] :ssl_ca_cert
      #   Same as the corresponding Client/Socket::SSL option.
      # @option options [ Array<OpenSSL::X509::Certificate> ] :ssl_ca_cert_object
      #   Same as the corresponding Client/Socket::SSL option.
      # @option options [ String ] :ssl_ca_cert_string
      #   Same as the corresponding Client/Socket::SSL option.
      # @option options [ String ] :ssl_cert
      #   Same as the corresponding Client/Socket::SSL option.
      # @option options [ OpenSSL::X509::Certificate ] :ssl_cert_object
      #   Same as the corresponding Client/Socket::SSL option.
      # @option options [ String ] :ssl_cert_string
      #   Same as the corresponding Client/Socket::SSL option.
      # @option options [ String ] :ssl_key
      #   Same as the corresponding Client/Socket::SSL option.
      # @option options [ OpenSSL::PKey ] :ssl_key_object
      #   Same as the corresponding Client/Socket::SSL option.
      # @option options [ String ] :ssl_key_pass_phrase
      #   Same as the corresponding Client/Socket::SSL option.
      # @option options [ String ] :ssl_key_string
      #   Same as the corresponding Client/Socket::SSL option.
      # @option options [ true, false ] :ssl_verify
      #   Same as the corresponding Client/Socket::SSL option.
      # @option options [ true, false ] :ssl_verify_certificate
      #   Same as the corresponding Client/Socket::SSL option.
      # @option options [ true, false ] :ssl_verify_hostname
      #   Same as the corresponding Client/Socket::SSL option.
      #
      # @return [ Mongo::Socket::SSL, Mongo::Socket::TCP ] The socket.
      #
      # @since 2.0.0
      # @api private
      def socket(socket_timeout, options = {})
        if options[:ssl]
          Socket::SSL.new(host, port, host_name, socket_timeout, Socket::PF_INET, options)
        else
          Socket::TCP.new(host, port, socket_timeout, Socket::PF_INET, options)
        end
      end
    end
  end
end
