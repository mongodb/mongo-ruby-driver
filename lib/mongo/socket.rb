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

require 'mongo/socket/ssl'
require 'mongo/socket/tcp'
require 'mongo/socket/unix'

module Mongo

  # Provides additional data around sockets for the driver's use.
  #
  # @since 2.0.0
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

    # @return [ Integer ] family The type of host family.
    attr_reader :family

    # @return [ Socket ] socket The wrapped socket.
    attr_reader :socket

    # @return [ Hash ] The options.
    attr_reader :options

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
        eof?
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
      @socket.close rescue true
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
      handle_errors { @socket.gets(*args) }
    end

    # Create the new socket for the provided family - ipv4, piv6, or unix.
    #
    # @example Create a new ipv4 socket.
    #   Socket.new(Socket::PF_INET)
    #
    # @param [ Integer ] family The socket domain.
    #
    # @since 2.0.0
    def initialize(family)
      @family = family
      @socket = ::Socket.new(family, SOCK_STREAM, 0)
      set_socket_options(@socket)
    end

    # Will read all data from the socket for the provided number of bytes.
    # If no data is returned, an exception will be raised.
    #
    # @example Read all the requested data from the socket.
    #   socket.read(4096)
    #
    # @param [ Integer ] length The number of bytes to read.
    #
    # @raise [ Mongo::SocketError ] If not all data is returned.
    #
    # @return [ Object ] The data from the socket.
    #
    # @since 2.0.0
    def read(length)
      handle_errors do
        data = read_from_socket(length)
        raise IOError unless (data.length > 0 || length == 0)
        while data.length < length
          chunk = read_from_socket(length - data.length)
          raise IOError unless (chunk.length > 0 || length == 0)
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
      handle_errors { @socket.readbyte }
    end

    # Writes data to the socket instance.
    #
    # @example Write to the socket.
    #   socket.write(data)
    #
    # @param [ Array<Object> ] args The data to be written.
    #
    # @return [ Integer ] The length of bytes written to the socket.
    #
    # @since 2.0.0
    def write(*args)
      handle_errors { @socket.write(*args) }
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

    def read_from_socket(length)
      # Just in case
      if length == 0
        return ''.force_encoding('BINARY')
      end

      deadline = (Time.now + timeout) if timeout

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
      rescue IO::WaitReadable
        select_timeout = (deadline - Time.now) if deadline
        if (select_timeout && select_timeout <= 0) || !Kernel::select([@socket], nil, [@socket], select_timeout)
          raise Timeout::Error.new("Took more than #{timeout} seconds to receive data.")
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

    def unix_socket?(sock)
      defined?(UNIXSocket) && sock.is_a?(UNIXSocket)
    end

    DEFAULT_TCP_KEEPINTVL = 10

    DEFAULT_TCP_KEEPCNT = 9

    DEFAULT_TCP_KEEPIDLE = 300

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

    def handle_errors
      begin
        yield
      rescue Errno::ETIMEDOUT => e
        raise Error::SocketTimeoutError, "#{e.class}: #{e} (for #{address})"
      rescue IOError, SystemCallError => e
        raise Error::SocketError, "#{e.class}: #{e} (for #{address})"
      rescue OpenSSL::SSL::SSLError => e
        raise Error::SocketError, "#{e.class}: #{e} (for #{address}) (#{SSL_ERROR})"
      end
    end

    def address
      raise NotImplementedError
    end
  end
end
