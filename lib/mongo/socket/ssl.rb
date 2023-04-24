# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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

    # Wrapper for TLS sockets.
    #
    # @since 2.0.0
    class SSL < Socket
      include OpenSSL

      # Initializes a new TLS socket.
      #
      # @example Create the TLS socket.
      #   SSL.new('::1', 27017, 30)
      #
      # @param [ String ] host The hostname or IP address.
      # @param [ Integer ] port The port number.
      # @param [ Float ] timeout The socket timeout value.
      # @param [ Integer ] family The socket family.
      # @param [ Hash ] options The options.
      #
      # @option options [ Float ] :connect_timeout Connect timeout.
      # @option options [ Address ] :connection_address Address of the
      #   connection that created this socket.
      # @option options [ Integer ] :connection_generation Generation of the
      #   connection (for non-monitoring connections) that created this socket.
      # @option options [ true | false ] :monitor Whether this socket was
      #   created by a monitoring connection.
      # @option options [ String ] :ssl_ca_cert The file containing concatenated
      #   certificate authority certificates used to validate certs passed from the
      #   other end of the connection. Intermediate certificates should NOT be
      #   specified in files referenced by this option. One of :ssl_ca_cert,
      #   :ssl_ca_cert_string or :ssl_ca_cert_object (in order of priority) is
      #   required when using :ssl_verify.
      # @option options [ Array<OpenSSL::X509::Certificate> ] :ssl_ca_cert_object
      #   An array of OpenSSL::X509::Certificate objects representing the
      #   certificate authority certificates used to validate certs passed from
      #   the other end of the connection. Intermediate certificates should NOT
      #   be specified in files referenced by this option. One of :ssl_ca_cert,
      #   :ssl_ca_cert_string or :ssl_ca_cert_object (in order of priority)
      #   is required when using :ssl_verify.
      # @option options [ String ] :ssl_ca_cert_string A string containing
      #   certificate authority certificate used to validate certs passed from the
      #   other end of the connection. This option allows passing only one CA
      #   certificate to the driver. Intermediate certificates should NOT
      #   be specified in files referenced by this option. One of :ssl_ca_cert,
      #   :ssl_ca_cert_string or :ssl_ca_cert_object (in order of priority) is
      #   required when using :ssl_verify.
      # @option options [ String ] :ssl_cert The certificate file used to identify
      #   the connection against MongoDB. A certificate chain may be passed by
      #   specifying the client certificate first followed by any intermediate
      #   certificates up to the CA certificate. The file may also contain the
      #   certificate's private key, which will be ignored. This option, if present,
      #   takes precedence over the values of :ssl_cert_string and :ssl_cert_object
      # @option options [ OpenSSL::X509::Certificate ] :ssl_cert_object The OpenSSL::X509::Certificate
      #   used to identify the connection against MongoDB. Only one certificate
      #   may be passed through this option.
      # @option options [ String ] :ssl_cert_string A string containing the PEM-encoded
      #   certificate used to identify the connection against MongoDB. A certificate
      #   chain may be passed by specifying the client certificate first followed
      #   by any intermediate certificates up to the CA certificate. The string
      #   may also contain the certificate's private key, which will be ignored,
      #   This option, if present, takes precedence over the value of :ssl_cert_object
      # @option options [ String ] :ssl_key The private keyfile used to identify the
      #   connection against MongoDB. Note that even if the key is stored in the same
      #   file as the certificate, both need to be explicitly specified. This option,
      #   if present, takes precedence over the values of :ssl_key_string and :ssl_key_object
      # @option options [ OpenSSL::PKey ] :ssl_key_object The private key used to identify the
      #   connection against MongoDB
      # @option options [ String ] :ssl_key_pass_phrase A passphrase for the private key.
      # @option options [ String ] :ssl_key_string A string containing the PEM-encoded private key
      #   used to identify the connection against MongoDB. This parameter, if present,
      #   takes precedence over the value of option :ssl_key_object
      # @option options [ true, false ] :ssl_verify Whether to perform peer certificate validation and
      #   hostname verification. Note that the decision of whether to validate certificates will be
      #   overridden if :ssl_verify_certificate is set, and the decision of whether to validate
      #   hostnames will be overridden if :ssl_verify_hostname is set.
      # @option options [ true, false ] :ssl_verify_certificate Whether to perform peer certificate
      #   validation. This setting overrides :ssl_verify with respect to whether certificate
      #   validation is performed.
      # @option options [ true, false ] :ssl_verify_hostname Whether to perform peer hostname
      #   validation. This setting overrides :ssl_verify with respect to whether hostname validation
      #   is performed.
      #
      # @since 2.0.0
      # @api private
      def initialize(host, port, host_name, timeout, family, options = {})
        super(timeout, options)
        @host, @port, @host_name = host, port, host_name
        @context = create_context(options)
        @family = family
        @tcp_socket = ::Socket.new(family, SOCK_STREAM, 0)
        begin
          @tcp_socket.setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)
          set_socket_options(@tcp_socket)
          run_tls_context_hooks

          connect!
        rescue
          @tcp_socket.close
          raise
        end
      end

      # @return [ SSLContext ] context The TLS context.
      attr_reader :context

      # @return [ String ] host The host to connect to.
      attr_reader :host

      # @return [ String ] host_name The original host name.
      attr_reader :host_name

      # @return [ Integer ] port The port to connect to.
      attr_reader :port

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
        Timeout.timeout(options[:connect_timeout], Error::SocketTimeoutError, "The socket took over #{options[:connect_timeout]} seconds to connect") do
          map_exceptions do
            @tcp_socket.connect(::Socket.pack_sockaddr_in(port, host))
          end
          @socket = OpenSSL::SSL::SSLSocket.new(@tcp_socket, context)
          begin
            @socket.hostname = @host_name
            @socket.sync_close = true
            map_exceptions do
              @socket.connect
            end
            verify_certificate!(@socket)
            verify_ocsp_endpoint!(@socket)
          rescue
            @socket.close
            @socket = nil
            raise
          end
          self
        end
      end
      private :connect!

      # Read a single byte from the socket.
      #
      # @example Read a single byte.
      #   socket.readbyte
      #
      # @return [ Object ] The read byte.
      #
      # @since 2.0.0
      def readbyte
        map_exceptions do
          byte = socket.read(1).bytes.to_a[0]
          byte.nil? ? raise(EOFError) : byte
        end
      end

      private

      def verify_certificate?
        # If ssl_verify_certificate is not present, disable only if
        # ssl_verify is explicitly set to false.
        if options[:ssl_verify_certificate].nil?
          options[:ssl_verify] != false
        # If ssl_verify_certificate is present, enable or disable based on its value.
        else
          !!options[:ssl_verify_certificate]
        end
      end

      def verify_hostname?
        # If ssl_verify_hostname is not present, disable only if ssl_verify is
        # explicitly set to false.
        if options[:ssl_verify_hostname].nil?
          options[:ssl_verify] != false
        # If ssl_verify_hostname is present, enable or disable based on its value.
        else
          !!options[:ssl_verify_hostname]
        end
      end

      def verify_ocsp_endpoint?
        if !options[:ssl_verify_ocsp_endpoint].nil?
          options[:ssl_verify_ocsp_endpoint] != false
        elsif !options[:ssl_verify_certificate].nil?
          options[:ssl_verify_certificate] != false
        else
          options[:ssl_verify] != false
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
            # In JRuby we must allow one call as this callback is invoked for
            # the initial connection also, not just for renegotiations -
            # https://github.com/jruby/jruby-openssl/issues/180
            if BSON::Environment.jruby?
              allowed_calls = 1
            else
              allowed_calls = 0
            end
            context.renegotiation_cb = lambda do |ssl|
              if allowed_calls <= 0
                raise RuntimeError, 'Client renegotiation disabled'
              end
              allowed_calls -= 1
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
            raise Error::SocketError, 'TLS handshake failed due to a hostname mismatch.'
          end
        end
      end

      def verify_ocsp_endpoint!(socket)
        unless verify_ocsp_endpoint?
          return
        end

        cert = socket.peer_cert
        ca_cert = socket.peer_cert_chain.last

        verifier = OcspVerifier.new(@host_name, cert, ca_cert, context.cert_store,
          **Utils.shallow_symbolize_keys(options))
        verifier.verify_with_cache
      end

      def read_buffer_size
        # Buffer size for TLS reads.
        # Capped at 16k due to https://linux.die.net/man/3/ssl_read
        16384
      end

      def human_address
        "#{host}:#{port} (#{host_name}:#{port}, TLS)"
      end

      def run_tls_context_hooks
        Mongo.tls_context_hooks.each do |hook|
          hook.call(@context)
        end
      end
    end
  end
end
