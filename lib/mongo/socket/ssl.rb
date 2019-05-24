# Copyright (C) 2014-2019 MongoDB, Inc.
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

      # @return [ Integer ] port The port to connect to.
      attr_reader :port

      # @return [ Float ] timeout The socket timeout.
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
        Timeout.timeout(options[:connect_timeout], Error::SocketTimeoutError) do
          handle_errors { @tcp_socket.connect(::Socket.pack_sockaddr_in(port, host)) }
          @socket = OpenSSL::SSL::SSLSocket.new(@tcp_socket, context)
          @socket.hostname = @host_name
          @socket.sync_close = true
          handle_errors { @socket.connect }
          verify_certificate!(@socket)
          self
        end
      end
      private :connect!

      # Initializes a new SSL socket.
      #
      # @example Create the SSL socket.
      #   SSL.new('::1', 27017, 30)
      #
      # @param [ String ] host The hostname or IP address.
      # @param [ Integer ] port The port number.
      # @param [ Float ] timeout The socket timeout value.
      # @param [ Integer ] family The socket family.
      # @param [ Hash ] options The options.
      #
      # @option options [ Float ] :connect_timeout Connect timeout.
      #
      # @since 2.0.0
      def initialize(host, port, host_name, timeout, family, options = {})
        @host, @port, @host_name, @timeout, @options = host, port, host_name, timeout, options
        @context = create_context(options)
        @family = family
        @tcp_socket = ::Socket.new(family, SOCK_STREAM, 0)
        @tcp_socket.setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)
        set_socket_options(@tcp_socket)
        connect!
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

      private

      def verify_certificate?
        @verify_certificate ||=
          # If ssl_verify_certificate is not present, disable only if ssl_verify is
          # explicitly set to false.
          if options[:ssl_verify_certificate].nil?
            options[:ssl_verify] != false
          # If ssl_verify_certificate is present, enable or disable based on its value.
          else
            !!options[:ssl_verify_certificate]
          end
      end

      def verify_hostname?
        @verify_hostname ||=
         # If ssl_verify_hostname is not present, disable only if ssl_verify is
          # explicitly set to false.
          if options[:ssl_verify_hostname].nil?
            options[:ssl_verify] != false
          # If ssl_verify_hostname is present, enable or disable based on its value.
          else
            !!options[:ssl_verify_hostname]
          end
      end


      def create_context(options)
        OpenSSL::SSL::SSLContext.new.tap do |context|
          if OpenSSL::SSL.const_defined?(:OP_NO_RENEGOTIATION)
            context.options = context.options | OpenSSL::SSL::OP_NO_RENEGOTIATION
          end

          if context.respond_to?(:renegotiation_cb=)
            # Disable renegotiation for older Ruby versions per the sample code at
            # https://rubydocs.org/d/ruby-2-6-0/classes/OpenSSL/SSL/SSLContext.html
            context.renegotiation_cb = lambda do |ssl|
              raise RuntimeError, 'Client renegotiation disabled'
            end
          end

          set_cert(context, options)
          set_key(context, options)

          if verify_certificate?
            context.verify_mode = OpenSSL::SSL::VERIFY_PEER
            set_cert_verification(context, options)
          else
            context.verify_mode = OpenSSL::SSL::VERIFY_NONE
          end

          if context.respond_to?(:verify_hostname=)
            # We manually check the hostname after the connection is established if necessary, so
            # we disable it here in order to give consistent errors across Ruby versions which
            # don't support hostname verification at the time of the handshake.
            context.verify_hostname = OpenSSL::SSL::VERIFY_NONE
          end
        end
      end

      def set_cert(context, options)
        if options[:ssl_cert]
          context.cert = OpenSSL::X509::Certificate.new(File.open(options[:ssl_cert]))
        elsif options[:ssl_cert_string]
          context.cert = OpenSSL::X509::Certificate.new(options[:ssl_cert_string])
        elsif options[:ssl_cert_object]
          context.cert = options[:ssl_cert_object]
        end
      end

      def set_key(context, options)
        passphrase = options[:ssl_key_pass_phrase]
        if options[:ssl_key]
          context.key = passphrase ? OpenSSL::PKey.read(File.open(options[:ssl_key]), passphrase) :
            OpenSSL::PKey.read(File.open(options[:ssl_key]))
        elsif options[:ssl_key_string]
          context.key = passphrase ? OpenSSL::PKey.read(options[:ssl_key_string], passphrase) :
            OpenSSL::PKey.read(options[:ssl_key_string])
        elsif options[:ssl_key_object]
          context.key = options[:ssl_key_object]
        end
      end

      def set_cert_verification(context, options)
        context.verify_mode = OpenSSL::SSL::VERIFY_PEER
        cert_store = OpenSSL::X509::Store.new
        if options[:ssl_ca_cert]
          cert_store.add_cert(OpenSSL::X509::Certificate.new(File.open(options[:ssl_ca_cert])))
        elsif options[:ssl_ca_cert_string]
          cert_store.add_cert(OpenSSL::X509::Certificate.new(options[:ssl_ca_cert_string]))
        elsif options[:ssl_ca_cert_object]
          raise TypeError("Option :ssl_ca_cert_object should be an array of OpenSSL::X509:Certificate objects") unless options[:ssl_ca_cert_object].is_a? Array
          options[:ssl_ca_cert_object].each {|cert| cert_store.add_cert(cert)}
        else
          cert_store.set_default_paths
        end
        context.cert_store = cert_store
      end

      def verify_certificate!(socket)
        if verify_hostname?
          unless OpenSSL::SSL.verify_certificate_identity(socket.peer_cert, host_name)
            raise Error::SocketError, 'SSL handshake failed due to a hostname mismatch.'
          end
        end
      end

      def read_buffer_size
        # Buffer size for SSL reads.
        # Capped at 16k due to https://linux.die.net/man/3/ssl_read
        16384
      end

      def address
        "#{host}:#{port} (#{host_name}:#{port}, TLS)"
      end
    end
  end
end
