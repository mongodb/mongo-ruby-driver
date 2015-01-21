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

require 'openssl'

module Mongo
  class Socket

    # Wrapper for SSL sockets.
    #
    # @since 2.0.0
    class SSL < Socket
      include OpenSSL

      # @return [ SSLContext ] context The ssl context.
      attr_reader :context

      # @return [ String ] host The host to connect to.
      attr_reader :host

      # @return [ Hash ] The ssl options.
      attr_reader :options

      # @return [ Integer ] port The port to connect to.
      attr_reader :port

      # @return [ Float ] timeout The connection timeout.
      attr_reader :timeout

      # Establishes a socket connection.
      #
      # @example Connect the socket.
      #   sock.connect!
      #
      # @note This method mutates the object by setting the socket
      #   internally.
      #
      # @return [ SSL ] The connected socket instance.
      #
      # @since 2.0.0
      def connect!
        Timeout.timeout(timeout, Mongo::SocketTimeoutError) do
          socket.setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)
          socket.connect(::Socket.pack_sockaddr_in(port, host))
          ssl_socket = OpenSSL::SSL::SSLSocket.new(socket, context)
          ssl_socket.sync_close = true
          ssl_socket.connect
          verify_certificate!(ssl_socket)
          self
        end
      end

      # Initializes a new SSL socket.
      #
      # @example Create the SSL socket.
      #   SSL.new('::1', 27017, 30)
      #
      # @param [ String ] host The hostname or IP address.
      # @param [ Integer ] port The port number.
      # @param [ Float ] timeout The socket timeout value.
      # @param [ Integer ] family The socket family.
      # @param [ Hash ] options The ssl options.
      #
      # @since 2.0.0
      def initialize(host, port, timeout, family, options = {})
        @host, @port, @timeout, @options = host, port, timeout, options
        @context = create_context(options)
        super(family)
      end

      private

      def create_context(options)
        context = OpenSSL::SSL::SSLContext.new
        if options[:ssl_cert]
          context.cert = OpenSSL::X509::Certificate.new(File.open(options[:ssl_cert]))
        end
        if options[:ssl_key]
          context.key = OpenSSL::PKey::RSA.new(File.open(options[:ssl_key]))
        end
        if options[:ssl_verify] || options[:ssl_ca_cert]
          context.ca_file = options[:ssl_ca_cert]
          context.verify_mode = OpenSSL::SSL::VERIFY_PEER
        end
        context
      end

      def verify_certificate!(socket)
        if context.verify_mode == OpenSSL::SSL::VERIFY_PEER
          unless OpenSSL::SSL.verify_certificate_identity(socket.peer_cert, host)
            raise Mongo::SocketError, 'SSL handshake failed due to a hostname mismatch.'
          end
        end
      end
    end
  end
end
