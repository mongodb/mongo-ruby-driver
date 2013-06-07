# Copyright (C) 2013 10gen Inc.
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

require 'socket'

module Mongo
  # Wrapper class for Socket
  #
  # Emulates UNIXSocket with operation and connection timeout
  # sans Timeout::timeout
  #
  class UNIXSocket < TCPSocket
    def initialize(socket_path, port=:socket, op_timeout=nil, connect_timeout=nil, opts={})
      @op_timeout      = op_timeout
      @connect_timeout = connect_timeout

      @address         = socket_path
      @port            = :socket # purposely override input

      @socket_address  = Socket.pack_sockaddr_un(@address)
      @socket          = Socket.new(Socket::AF_UNIX, Socket::SOCK_STREAM, 0)
      connect
    end
  end
end

