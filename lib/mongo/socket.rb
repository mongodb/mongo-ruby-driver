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

require 'mongo/socket/ssl'
require 'mongo/socket/tcp'
require 'mongo/socket/unix'

module Mongo

  # Provides additional data around sockets for the driver's use.
  #
  # @since 2.0.0
  # @api private
  class Socket
    include ::Socket::Constants

    # Error message for SSL related exceptions.
    #
    # @since 2.0.0
    SSL_ERROR = 'MongoDB may not be configured with SSL support'.freeze

    # Error message for timeouts on socket calls.
    #
    # @since 2.0.0
    # @deprecated
    TIMEOUT_ERROR = 'Socket request timed out'.freeze

    # The pack directive for timeouts.
    #
    # @since 2.0.0
    TIMEOUT_PACK = 'l_2'.freeze

    # Write data to the socket in chunks of this size.
    #
    # @api private
    WRITE_CHUNK_SIZE = 65536

    # Initializes common socket attributes.
    #
    # @param [ Float ] timeout The socket timeout value.
    # @param [ Hash ] options The options.
    #
    # @option options [ Float ] :connect_timeout Connect timeout.
    # @option options [ Address ] :connection_address Address of the
    #   connection that created this socket.
    # @option options [ Integer ] :connection_generation Generation of the
    #   connection (for non-monitoring connections) that created this socket.
    # @option options [ true | false ] :monitor Whether this socket was
    #   created by a monitoring connection.
    #
    # @api private
    def initialize(timeout, options)
      @timeout = timeout
      @options = options
    end

    # @return [ Integer ] family The type of host family.
    attr_reader :family

    # @return [ Socket ] socket The wrapped socket.
    attr_reader :socket

    # @return [ Hash ] The options.
    attr_reader :options

    # @return [ Float ] timeout The socket timeout.
    attr_reader :timeout

    # @return [ Address ] Address of the connection that created this socket.
    #
    # @api private
    def connection_address
      options[:connection_address]
    end

    # @return [ Integer ] Generation of the connection (for non-monitoring
    #   connections) that created this socket.
    #
    # @api private
    def connection_generation
      options[:connection_generation]
    end

    # @return [ true | false ] Whether this socket was created by a monitoring
    #   connection.
    #
    # @api private
    def monitor?
      !!options[:monitor]
    end

    # @return [ String ] Human-readable summary of the socket for debugging.
    #
    # @api private
    def summary
      fileno = @socket&.fileno rescue '<no socket>' || '<no socket>'
      if monitor?
        indicator = if options[:push]
          'pm'
        else
          'm'
        end
        "#{connection_address};#{indicator};fd=#{fileno}"
      else
        "#{connection_address};c:#{connection_generation};fd=#{fileno}"
      end
    end

    # Is the socket connection alive?
    #
    # @example Is the socket alive?
    #   socket.alive?
    #
    # @return [ true, false ] If the socket is alive.
    #
    # @deprecated Use #connectable? on the connection instead.
    def alive?
      sock_arr = [ @socket ]
      if Kernel::select(sock_arr, nil, sock_arr, 0)
        # The eof? call is supposed to return immediately since select
        # indicated the socket is readable. However, if @socket is an SSL
        # socket, eof? can block anyway - see RUBY-2140.
        begin
          Timeout.timeout(0.1) do
            eof?
          end
        rescue ::Timeout::Error
          true
        end
      else
        true
      end
    end

    # Close the socket.
    #
    # @example Close the socket.
    #   socket.close
    #
    # @return [ true ] Always true.
    #
    # @since 2.0.0
    def close
      @socket.close rescue nil
      true
    end

    # Delegates gets to the underlying socket.
    #
    # @example Get the next line.
    #   socket.gets(10)
    #
    # @param [ Array<Object> ] args The arguments to pass through.
    #
    # @return [ Object ] The returned bytes.
    #
    # @since 2.0.0
    def gets(*args)
      map_exceptions do
        @socket.gets(*args)
      end
    end

    # Will read all data from the socket for the provided number of bytes.
    # If no data is returned, an exception will be raised.
    #
    # @example Read all the requested data from the socket.
    #   socket.read(4096)
    #
    # @param [ Integer ] length The number of bytes to read.
    # @param [ Numeric ] timeout The timeout to use for each chunk read.
    #
    # @raise [ Mongo::SocketError ] If not all data is returned.
    #
    # @return [ Object ] The data from the socket.
    #
    # @since 2.0.0
    def read(length, timeout: nil)
      map_exceptions do
        data = read_from_socket(length, timeout: timeout)
        unless (data.length > 0 || length == 0)
          raise IOError, "Expected to read > 0 bytes but read 0 bytes"
        end
        while data.length < length
          chunk = read_from_socket(length - data.length, timeout: timeout)
          unless (chunk.length > 0 || length == 0)
            raise IOError, "Expected to read > 0 bytes but read 0 bytes"
          end
          data << chunk
        end
        data
      end
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
      map_exceptions do
        @socket.readbyte
      end
    end

    # Writes data to the socket instance.
    #
    # @param [ Array<Object> ] args The data to be written.
    #
    # @return [ Integer ] The length of bytes written to the socket.
    #
    # @since 2.0.0
    def write(*args)
      map_exceptions do
        do_write(*args)
      end
    end

    # Tests if this socket has reached EOF. Primarily used for liveness checks.
    #
    # @since 2.0.5
    def eof?
      @socket.eof?
    rescue IOError, SystemCallError
      true
    end

    # For backwards compatibilty only, do not use.
    #
    # @return [ true ] Always true.
    #
    # @deprecated
    def connectable?
      true
    end

    private

    def read_from_socket(length, timeout: nil)
      # Just in case
      if length == 0
        return ''.force_encoding('BINARY')
      end

      _timeout = timeout || self.timeout
      if _timeout
        if _timeout > 0
          deadline = Time.now + _timeout
        elsif _timeout < 0
          raise Errno::ETIMEDOUT, "Negative timeout #{_timeout} given to socket"
        end
      end

      # We want to have a fixed and reasonably small size buffer for reads
      # because, for example, OpenSSL reads in 16 kb chunks max.
      # Having a 16 mb buffer means there will be 1000 reads each allocating
      # 16 mb of memory and using 16 kb of it.
      buf_size = read_buffer_size
      data = nil

      # If we want to read less than the buffer size, just allocate the
      # memory that is necessary
      if length < buf_size
        buf_size = length
      end

      # The binary encoding is important, otherwise Ruby performs encoding
      # conversions of some sort during the write into the buffer which
      # kills performance
      buf = allocate_string(buf_size)
      retrieved = 0
      begin
        while retrieved < length
          retrieve = length - retrieved
          if retrieve > buf_size
            retrieve = buf_size
          end
          chunk = @socket.read_nonblock(retrieve, buf)

          # If we read the entire wanted length in one operation,
          # return the data as is which saves one memory allocation and
          # one copy per read
          if retrieved == 0 && chunk.length == length
            return chunk
          end

          # If we are here, we are reading the wanted length in
          # multiple operations. Allocate the total buffer here rather
          # than up front so that the special case above won't be
          # allocating twice
          if data.nil?
            data = allocate_string(length)
          end

          # ... and we need to copy the chunks at this point
          data[retrieved, chunk.length] = chunk
          retrieved += chunk.length
        end
      # As explained in https://ruby-doc.com/core-trunk/IO.html#method-c-select,
      # reading from a TLS socket may require writing which may raise WaitWritable
      rescue IO::WaitReadable, IO::WaitWritable => exc
        if deadline
          select_timeout = deadline - Time.now
          if select_timeout <= 0
            raise Errno::ETIMEDOUT, "Took more than #{_timeout} seconds to receive data"
          end
        end
        if exc.is_a?(IO::WaitReadable)
          select_args = [[@socket], nil, [@socket], select_timeout]
        else
          select_args = [nil, [@socket], [@socket], select_timeout]
        end
        unless Kernel::select(*select_args)
          raise Errno::ETIMEDOUT, "Took more than #{_timeout} seconds to receive data"
        end
        retry
      end

      data
    end

    def allocate_string(capacity)
      if RUBY_VERSION >= '2.4.0'
        String.new('', :capacity => capacity, :encoding => 'BINARY')
      else
        ('x'*capacity).force_encoding('BINARY')
      end
    end

    def read_buffer_size
      # Buffer size for non-SSL reads
      # 64kb
      65536
    end

    # Writes data to the socket instance.
    #
    # This is a separate method from +write+ for ease of mocking in the tests.
    # This method should not perform any exception mapping, upstream code
    # sholud map exceptions.
    #
    # @param [ Array<Object> ] args The data to be written.
    #
    # @return [ Integer ] The length of bytes written to the socket.
    def do_write(*args)
      # This method used to forward arguments to @socket.write in a
      # single call like so:
      #
      # @socket.write(*args)
      #
      # Turns out, when each buffer to be written is large (e.g. 32 MiB),
      # this write call would take an extremely long time (20+ seconds)
      # while using 100% CPU. Splitting the writes into chunks produced
      # massively better performance (0.05 seconds to write the 32 MiB of
      # data on the same hardware). Unfortunately splitting the data,
      # one would assume, results in it being copied, but this seems to be
      # a much more minor issue compared to CPU cost of writing large buffers.
      args.each do |buf|
        buf = buf.to_s
        i = 0
        while i < buf.length
          chunk = buf[i...i+WRITE_CHUNK_SIZE]
          @socket.write(chunk)
          i += WRITE_CHUNK_SIZE
        end
      end
    end

    def unix_socket?(sock)
      defined?(UNIXSocket) && sock.is_a?(UNIXSocket)
    end

    DEFAULT_TCP_KEEPINTVL = 10

    DEFAULT_TCP_KEEPCNT = 9

    DEFAULT_TCP_KEEPIDLE = 120

    def set_keepalive_opts(sock)
      sock.setsockopt(SOL_SOCKET, SO_KEEPALIVE, true)
      set_option(sock, :TCP_KEEPINTVL, DEFAULT_TCP_KEEPINTVL)
      set_option(sock, :TCP_KEEPCNT, DEFAULT_TCP_KEEPCNT)
      set_option(sock, :TCP_KEEPIDLE, DEFAULT_TCP_KEEPIDLE)
    rescue
    end

    def set_option(sock, option, default)
      if Socket.const_defined?(option)
        system_default = sock.getsockopt(IPPROTO_TCP, option).int
        if system_default > default
          sock.setsockopt(IPPROTO_TCP, option, default)
        end
      end
    end

    def set_socket_options(sock)
      sock.set_encoding(BSON::BINARY)
      set_keepalive_opts(sock)
    end

    def map_exceptions
      begin
        yield
      rescue Errno::ETIMEDOUT => e
        raise Error::SocketTimeoutError, "#{e.class}: #{e} (for #{human_address})"
      rescue IOError, SystemCallError => e
        raise Error::SocketError, "#{e.class}: #{e} (for #{human_address})"
      rescue OpenSSL::SSL::SSLError => e
        raise Error::SocketError, "#{e.class}: #{e} (for #{human_address}) (#{SSL_ERROR})"
      end
    end

    def human_address
      raise NotImplementedError
    end
  end
end
