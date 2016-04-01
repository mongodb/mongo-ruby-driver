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

      # @return [ String ] host_name The original host name.
      attr_reader :host_name

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
        Timeout.timeout(timeout, Error::SocketTimeoutError) do
          handle_errors { @tcp_socket.connect(::Socket.pack_sockaddr_in(port, host)) }
          @socket = OpenSSL::SSL::SSLSocket.new(@tcp_socket, context)
          @socket.sync_close = true
          handle_errors { @socket.connect }
          verify_certificate!(@socket)
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
      def initialize(host, port, host_name, timeout, family, options = {})
        @host, @port, @host_name, @timeout, @options = host, port, host_name, timeout, options
        @context = create_context(options)
        @family = family
        @tcp_socket = ::Socket.new(family, SOCK_STREAM, 0)
        @tcp_socket.setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)
        set_socket_options(@tcp_socket)
      end

      # Read a single byte from the socket.
      #
      # @example Read a single byte.
      #   socket.readbyte
      #
      # @return [ Object ] The read byte.
      #
      # @since 2.0.0
      def readbyte
        handle_errors do
          byte = socket.read(1).bytes.to_a[0]
          byte.nil? ? raise(EOFError) : byte
        end
      end

      # This socket can only be used if the ssl socket (@socket) has been created.
      #
      # @example Is the socket connectable?
      #   socket.connectable?
      #
      # @return [ true, false ] If the socket is connectable.
      #
      # @since 2.2.5
      def connectable?
        !!@socket
      end

      private

      def create_context(options)
        context = OpenSSL::SSL::SSLContext.new
        set_cert(context, options) if options[:ssl_cert]
        set_key(context, options) if options[:ssl_key]
        set_cert_verification(context, options) unless options[:ssl_verify] == false
        context
      end

      def set_cert(context, options)
        context.cert = OpenSSL::X509::Certificate.new(File.open(options[:ssl_cert]))
      end

      def set_key(context, options)
        if options[:ssl_key_pass_phrase]
          context.key = OpenSSL::PKey::RSA.new(File.open(options[:ssl_key]),
                                               options[:ssl_key_pass_phrase])
        else
          context.key = OpenSSL::PKey::RSA.new(File.open(options[:ssl_key]))
        end
      end

      def set_cert_verification(context, options)
        context.verify_mode = OpenSSL::SSL::VERIFY_PEER
        cert_store = OpenSSL::X509::Store.new
        if options[:ssl_ca_cert]
          cert_store.add_file(options[:ssl_ca_cert])
        else
          cert_store.set_default_paths
        end
        context.cert_store = cert_store
      end

      def verify_certificate!(socket)
        if context.verify_mode == OpenSSL::SSL::VERIFY_PEER
          unless OpenSSL::SSL.verify_certificate_identity(socket.peer_cert, host_name)
            raise Error::SocketError, 'SSL handshake failed due to a hostname mismatch.'
          end
        end
      end
    end
  end
end
