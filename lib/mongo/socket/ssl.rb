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
        # Since we clear cert_text during processing, we need to examine
        # ssl_cert_object here to avoid considering it if we have also
        # processed the text.
        if options[:ssl_cert]
          cert_text = File.read(options[:ssl_cert])
          cert_object = nil
        elsif cert_text = options[:ssl_cert_string]
          cert_object = nil
        else
          cert_object = options[:ssl_cert_object]
        end

        # The client certificate may be a single certificate or a bundle
        # (client certificate followed by intermediate certificates).
        # The text may also include private keys for the certificates.
        # OpenSSL supports passing the entire bundle as a certificate chain
        # to the context via SSL_CTX_use_certificate_chain_file, but the
        # Ruby openssl extension does not currently expose this functionality
        # per https://github.com/ruby/openssl/issues/254.
        # Therefore, extract the individual certificates from the certificate
        # text, and if there is more than one certificate provided, use
        # extra_chain_cert option to add the intermediate ones. This
        # implementation is modeled after
        # https://github.com/venuenext/ruby-kafka/commit/9495f5daf254b43bc88062acad9359c5f32cb8b5.
        # Note that the parsing here is not identical to what OpenSSL employs -
        # for instance, if there is no newline between two certificates
        # this code will extract them both but OpenSSL fails in this situation.
        if cert_text
          certs = cert_text.scan(/-----BEGIN CERTIFICATE-----(?:.|\n)+?-----END CERTIFICATE-----/)
          if certs.length > 1
            context.cert = OpenSSL::X509::Certificate.new(certs.shift)
            context.extra_chain_cert = certs.map do |cert|
              OpenSSL::X509::Certificate.new(cert)
            end
            # All certificates are already added to the context, skip adding
            # them again below.
            cert_text = nil
          end
        end

        if cert_text
          context.cert = OpenSSL::X509::Certificate.new(cert_text)
        elsif cert_object
          context.cert = cert_object
        end
      end

      def set_key(context, options)
        passphrase = options[:ssl_key_pass_phrase]
        if options[:ssl_key]
          context.key = load_private_key(File.read(options[:ssl_key]), passphrase)
        elsif options[:ssl_key_string]
          context.key = load_private_key(options[:ssl_key_string], passphrase)
        elsif options[:ssl_key_object]
          context.key = options[:ssl_key_object]
        end
      end

      def load_private_key(text, passphrase)
        args = if passphrase
          [text, passphrase]
        else
          [text]
        end
        # On JRuby, PKey.read does not grok cert+key bundles.
        # https://github.com/jruby/jruby-openssl/issues/176
        if BSON::Environment.jruby?
          [OpenSSL::PKey::RSA, OpenSSL::PKey::DSA].each do |cls|
            begin
              return cls.send(:new, *args)
            rescue OpenSSL::PKey::PKeyError
              # ignore
            end
          end
          # Neither RSA nor DSA worked, fall through to trying PKey
        end
        OpenSSL::PKey.send(:read, *args)
      end

      def set_cert_verification(context, options)
        context.verify_mode = OpenSSL::SSL::VERIFY_PEER
        cert_store = OpenSSL::X509::Store.new
        if options[:ssl_ca_cert]
          cert_store.add_file(options[:ssl_ca_cert])
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
