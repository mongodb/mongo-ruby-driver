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

require 'mongo/pool/socket/ssl/context'

module Mongo
  class Pool
    module Socket

      # Wrapper for SSL sockets
      class SSL
        include Socket::Connectable
        include OpenSSL

        # @return [ OpenSSL::SSL::SSLContext ] context The SSL context.
        attr_reader :context

        # Establishes the socket connection and performs
        # optional SSL valiation.
        #
        # @example Connect the SSL socket.
        #   sock.connect
        #
        # @note This method is mutable in that the wrapped socket is set.
        #
        # @return [ SSL ] The connected socket instance.
        #
        # @since 2.0.0
        def connect!
          initialize! do
            # Apply ssl wrapper and perform handshake.
            ssl_socket = OpenSSL::SSL::SSLSocket.new(@socket, context)
            ssl_socket.sync_close = true
            ssl_socket.connect

            # Perform peer cert validation if needed.
            if verifying_certificate?
              unless OpenSSL::SSL.verify_certificate_identity(ssl_socket.peer_cert, host)
                raise Mongo::SocketError, 'SSL handshake failed due to a hostname mismatch.'
              end
            end
          end
        end

        # Initializes a new TCP socket with SSL support.
        #
        # @example Create the new SSL socket.
        #   SSL.new('::1', 30, 27017)
        #   SSL.new('127.0.0.1', 30, 27017)
        #   SSL.new('127.0.0.1', 30, 27017)
        #
        # @param [ String ] host The hostname or IP address.
        # @param [ Integer ] port The port number.
        # @param [ Float ] timeout The socket timeout value.
        # @param [ Integer ] family The socket family.
        # @param [ Hash ] options Optional settings and configuration values.
        #
        # @option options [ true, false ] :connect (true) If true calls connect
        #   before returning the object instance.
        # @option options [ String ] :ssl_cert (nil) Path to the certificate file
        #   used to identify the local connection against MongoDB.
        # @option options [ String ] :ssl_key (nil) Path to the private key file
        #   used to identify the local connection against MongoDB. If included
        #   in the ssl certificate file then only :ssl_cert is needed.
        # @option options [ true, false ] :ssl_verify (nil) Specifies whether or
        #   not peer certificate validation should occur.
        # @option options [ String ] :ssl_ca_cert (nil) Path to the :ca_certs file
        #   containing a set of concatenated "certification authority"
        #   certificates, which are used to validate the certificates returned
        #   from the other end of the socket connection. Implies :ssl_verify.
        #
        # @since 2.0.0
        def initialize(host, port, timeout, family, options = {})
          @host    = host
          @port    = port
          @timeout = timeout
          @family  = family
          @context = Context.create(options)
        end

        # Does this socket verify it's certificate on connection?
        #
        # @example Is the socket verifying it's certificate?
        #   socket.verifying_certificate?
        #
        # @return [ true, false ] If the certificate is verified.
        #
        # @since 2.0.0
        def verifying_certificate?
          context.verify_mode == OpenSSL::SSL::VERIFY_PEER
        end
      end
    end
  end
end
