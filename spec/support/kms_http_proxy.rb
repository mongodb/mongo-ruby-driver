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
    wait_until_ready(port, tls: !ca_file.nil?)
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
  def connect_callback(proxy_host:, proxy_port:)
    lambda do |host, port, _timeout_ms|
      socket = ::TCPSocket.new(proxy_host, proxy_port)
      socket.write("CONNECT #{host}:#{port} HTTP/1.1\r\nHost: #{host}:#{port}\r\n\r\n")
      status = socket.gets
      unless status && status.start_with?('HTTP/1.1 200')
        socket.close
        raise "proxy CONNECT failed: #{status.inspect}"
      end
      # Consume the remaining response headers up to the blank line.
      loop do
        line = socket.gets
        break if line.nil? || line == "\r\n"
      end
      socket
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
