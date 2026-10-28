# frozen_string_literal: true

require 'net/http'
require 'socket'

# Starts and drives the vendored KMS HTTP CONNECT proxy used by the
# kms_connect_callback integration specs.
module KmsHttpProxy
  extend self

  def script_path
    File.expand_path(
      '../../.mod/drivers-evergreen-tools/.evergreen/csfle/kms_http_proxy.py',
      __dir__
    )
  end

  def available?
    File.exist?(script_path)
  end

  # Starts the proxy and waits until its /metrics endpoint answers.
  #
  # @return [ Integer ] the process id.
  def start(port:, ca_file: nil, cert_file: nil)
    args = [ 'python3', script_path, '--host', '127.0.0.1', '--port', port.to_s ]
    args += [ '--ca_file', ca_file ] if ca_file
    args += [ '--cert_file', cert_file ] if cert_file
    pid = Process.spawn(*args, out: File::NULL, err: File::NULL)
    begin
      wait_until_ready(port, tls: !ca_file.nil?)
    rescue StandardError
      # Reap the child so a failed startup does not leave a process holding the
      # port, which would break subsequent runs.
      stop(pid)
      raise
    end
    pid
  end

  def stop(pid)
    Process.kill('TERM', pid)
    Process.wait(pid)
  rescue Errno::ESRCH, Errno::ECHILD
    nil
  end

  def reset(port:, tls: false)
    request(port, tls: tls, method: Net::HTTP::Post, path: '/reset')
  end

  def connect_count(port:, tls: false)
    body = request(port, tls: tls, method: Net::HTTP::Get, path: '/metrics')
    body[/connect_count (\d+)/, 1].to_i
  end

  # Returns a callback suitable for :kms_connect_callback that tunnels through
  # the proxy using an HTTP CONNECT request.
  #
  # @param [ String ] proxy_host The host the proxy listens on.
  # @param [ Integer ] proxy_port The port the proxy listens on.
  # @param [ Boolean ] tls Whether the proxy speaks TLS on its inbound
  #   connection (that is, it was started with --ca_file/--cert_file). When
  #   true the CONNECT handshake runs over a TLS connection to the proxy, and
  #   the returned socket is one end of a socketpair bridged to the proxy by a
  #   background thread. The bridge is required because the driver wraps the
  #   returned object in TLS to the KMS host, and Ruby's OpenSSL::SSL::SSLSocket
  #   cannot wrap another SSLSocket; the returned object must be fd-backed.
  #
  # @return [ Proc ] a callback invoked as +call(host, port, timeout_ms)+.
  def connect_callback(proxy_host:, proxy_port:, tls: false)
    lambda do |host, port, _timeout_ms|
      proxy_socket = open_proxy_socket(proxy_host, proxy_port, tls: tls)
      begin
        perform_connect(proxy_socket, host, port)
      rescue StandardError
        proxy_socket.close
        raise
      end
      tls ? bridge_to_socket_pair(proxy_socket) : proxy_socket
    end
  end

  # Opens a connection to the proxy, wrapping it in TLS when the proxy was
  # started in TLS mode.
  def open_proxy_socket(proxy_host, proxy_port, tls:)
    tcp_socket = ::TCPSocket.new(proxy_host, proxy_port)
    return tcp_socket unless tls

    begin
      context = OpenSSL::SSL::SSLContext.new
      context.ca_file = File.expand_path('../../.evergreen/x509gen/ca.pem', __dir__)
      # The local proxy presents a self-signed certificate, so chain
      # verification cannot succeed; this harness only needs an encrypted
      # channel to the proxy.
      context.verify_mode = OpenSSL::SSL::VERIFY_NONE
      ssl_socket = OpenSSL::SSL::SSLSocket.new(tcp_socket, context)
      ssl_socket.sync_close = true
      ssl_socket.connect
      ssl_socket
    rescue StandardError
      tcp_socket.close
      raise
    end
  end

  # Performs the HTTP CONNECT handshake over +socket+ and leaves it positioned
  # at the start of the tunneled byte stream.
  def perform_connect(socket, host, port)
    socket.write("CONNECT #{host}:#{port} HTTP/1.1\r\nHost: #{host}:#{port}\r\n\r\n")
    status = socket.gets
    unless status && status.start_with?('HTTP/1.1 200')
      raise "proxy CONNECT failed: #{status.inspect}"
    end

    # Consume the remaining response headers up to the blank line.
    loop do
      line = socket.gets
      break if line.nil? || line == "\r\n"
    end
  end

  # Returns one end of a socketpair whose other end is bridged to
  # +proxy_socket+ by a background thread. The driver wraps the returned socket
  # in TLS to the KMS host; the bridge relays those bytes to the proxy.
  def bridge_to_socket_pair(proxy_socket)
    client_socket, bridge_socket = Socket.pair(:UNIX, :STREAM, 0)
    pump_bytes(bridge_socket, proxy_socket)
    client_socket
  end

  # Pumps bytes in both directions between two connected sockets until either
  # side closes, then closes both.
  def pump_bytes(socket_a, socket_b)
    [ [ socket_a, socket_b ], [ socket_b, socket_a ] ].each do |source, destination|
      Thread.new do
        loop { destination.write(source.readpartial(16_384)) }
      rescue StandardError
        nil
      ensure
        [ socket_a, socket_b ].each do |socket|
          socket.close
        rescue StandardError
          nil
        end
      end
    end
  end

  def wait_until_ready(port, tls:, deadline: Time.now + 15)
    loop do
      connect_count(port: port, tls: tls)
      return
    rescue StandardError
      raise "kms_http_proxy did not become ready on port #{port}" if Time.now > deadline

      sleep 0.2
    end
  end

  def request(port, tls:, method:, path:)
    http = Net::HTTP.new('127.0.0.1', port)
    http.use_ssl = tls
    if tls
      http.verify_mode = OpenSSL::SSL::VERIFY_NONE
      http.ca_file = File.expand_path('../../.evergreen/x509gen/ca.pem', __dir__)
    end
    http.request(method.new(path)).body
  end
end
