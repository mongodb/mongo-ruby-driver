# Copyright (C) 2009-2013 MongoDB, Inc.
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
  module Pool
    module Socket

      # Wrapper for SSL sockets
      class SSL

        include Socket::Base
        include OpenSSL

        # Initializes a new TCP socket with SSL support.
        #
        # @example
        #   SSL.new('::1', 30, 27017)
        #   SSL.new('127.0.0.1', 30, 27017)
        #   SSL.new('127.0.0.1', 30, 27017, :connect => false)
        #
        # @param host [String] The hostname or IP address.
        # @param port [Integer] The port number.
        # @param timeout [Integer] The socket timeout value.
        # @param opts [Hash] Optional settings and configuration values.
        #
        # @option opts [true, false] :connect (true) If true calls connect
        #   before returning the object instance.
        # @option opts [String] :ssl_cert (nil) Path to the certificate file
        #   used to identify the local connection against MongoDB.
        # @option opts [String] :ssl_key (nil) Path to the private key file
        #   used to identify the local connection against MongoDB. If included
        #   in the ssl certificate file then only :ssl_cert is needed.
        # @option opts [true, false] :ssl_verify (nil) Specifies whether or
        #   not peer certificate validation should occur.
        # @option opts [String] :ssl_ca_cert (nil) Path to the :ca_certs file
        #   containing a set of concatenated "certification authority"
        #   certificates, which are used to validate the certificates returned
        #   from the other end of the socket connection. Implies :ssl_verify.
        #
        # @return [SSL] The SSL socket instance.
        def initialize(host, port, timeout, opts = {})
          @host    = host
          @port    = port
          @timeout = timeout

          @context = OpenSSL::SSL::SSLContext.new

          # client SSL certificate
          if opts[:ssl_cert]
            @context.cert =
              OpenSSL::X509::Certificate.new(File.open(opts[:ssl_cert]))
          end

          # client private key file (optional if included in cert)
          if opts[:ssl_key]
            @context.key = OpenSSL::PKey::RSA.new(File.open(opts[:ssl_key]))
          end

          # peer certificate validation
          if opts[:ssl_verify] || opts[:ssl_ca_cert]
            @ssl_verify          = true
            @context.ca_file     = opts[:ca_cert]
            @context.verify_mode = OpenSSL::SSL::VERIFY_PEER
          end

          connect if opts.fetch(:connect, true)
          self
        end

        # Establishes the socket connection and performs
        # optional SSL valiation.
        #
        # @example
        #   sock = SSL.new('::1', 27017, 30)
        #   sock.connect
        #
        # @return [Socket] The connected socket instance.
        def connect
          Timeout.timeout(@timeout, Mongo::SocketTimeoutError) do
            @socket = handle_connect

            # apply ssl wrapper and perform handshake
            @ssl_socket = OpenSSL::SSL::SSLSocket.new(@socket, @context)
            @ssl_socket.sync_close = true
            @ssl_socket.connect

            # perform peer cert validation if needed
            if @ssl_verify
              unless OpenSSL::SSL.verify_certificate_identity(
                @ssl_socket.peer_cert, @host)

                raise Mongo::SocketError, 'SSL handshake failed due ' +
                                          'to a hostname mismatch.'
              end
            end
          end
        end

      end

    end
  end
end
