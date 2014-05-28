# Copyright (C) 2009-2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'resolv'

module Mongo
  class Server
    class Address

      # Provides common behaviour between IPv4, IPv6, and socket address
      # resolution.
      #
      # @since 2.0.0
      module Resolvable

        # @return [ String ] host The original host name.
        attr_reader :host

        # @return [ String ] ip The resolved ip address.
        attr_reader :ip

        # @return [ Integer ] port The port.
        attr_reader :port

        # Resolve the ip address. Will ensure that the resolved ip matches the
        # appropriate ip type.
        #
        # @example Resolve the ip address.
        #   resolvable.resolve!
        #
        # @return [ String ] The ip address.
        #
        # @since 2.0.0
        def resolve!
          Resolv.each_address(host) do |address|
            return @ip = address if address =~ pattern
          end
        end
      end
    end
  end
end

