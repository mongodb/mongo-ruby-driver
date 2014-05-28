# Copyright (C) 2009-2014 MongoDB, Inc.
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
require 'openssl'
require 'timeout'

module Mongo
  class Pool
    module Socket

      # Module for behavior common across all supported socket types.
      module Connectable
        include ::Socket::Constants

        # The pack directive for timeouts.
        #
        # @since 2.0.0
        TIMEOUT_PACK = 'l_2'.freeze

        # @return [ Integer ] family The socket family (IPv4, IPv6, Unix).
        attr_reader :family

        # @return [ String ] host The host to connect to.
        attr_reader :host

        # @return [ Integer ] port The port to connect to.
        attr_reader :port

        # @return [ Float ] timeout The connection timeout.
        attr_reader :timeout

        def initialize!
          @socket = initialize_socket
          yield if block_given?
          self
        end

        def initialize_socket
          Timeout.timeout(timeout, Mongo::SocketTimeoutError) do
            sock = default_socket
            sock.setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)
            # @todo: durran: This is where I deadlock at random.
            sock.connect(::Socket.pack_sockaddr_in(port, host))
            sock
          end
        end
      end
    end
  end
end
