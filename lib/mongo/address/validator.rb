# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2017-2020 MongoDB Inc.
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

module Mongo

  class Address

    # @api private
    module Validator

      # Takes an address string in ipv4/ipv6/hostname/socket path format and
      # validates its format.
      def validate_address_str!(address_str)
        case address_str
        when /\A\[[\d:]+\](?::(\d+))?\z/
          # ipv6 with optional port
          if port_str = $1
            validate_port_str!(port_str)
          end
        when /\A\//, /\.sock\z/
          # Unix socket path.
          # Spec requires us to validate that the path has no unescaped
          # slashes, but if this were to be the case, parsing would have
          # already failed elsewhere because the URI would've been split in
          # a weird place.
          # The spec also allows relative socket paths and requires that
          # socket paths end in ".sock". We accept all paths but special case
          # the .sock extension to avoid relative paths falling into the
          # host:port case below.
        when /[\/\[\]]/
          # Not a host:port nor an ipv4 address with optional port.
          # Possibly botched ipv6 address with e.g. port delimiter present and
          # port missing, or extra junk before or after.
          raise Error::InvalidAddress,
            "Invalid hostname: #{address_str}"
        when /:.*:/m
          raise Error::InvalidAddress,
            "Multiple port delimiters are not allowed: #{address_str}"
        else
          # host:port or ipv4 address with optional port number
          host, port = address_str.split(':')
          if host.empty?
            raise Error::InvalidAddress, "Host is empty: #{address_str}"
          end

          validate_hostname!(host)

          if port && port.empty?
            raise Error::InvalidAddress, "Port is empty: #{address_str}"
          end

          validate_port_str!(port)
        end
      end

      private

      # Validates format of the hostname, in particular for further use as
      # the origin in same origin verification.
      #
      # The hostname must have been normalized to remove the trailing dot if
      # it was obtained from a DNS record. This method prohibits trailing dots.
      def validate_hostname!(host)
        # Since we are performing same origin verification during SRV
        # processing, prohibit leading dots in hostnames, trailing dots
        # and runs of multiple dots. DNS resolution of SRV records yields
        # hostnames with trailing dots, those trailing dots are removed
        # during normalization process prior to validation.
        if host.start_with?('.')
          raise Error::InvalidAddress, "Hostname cannot start with a dot: #{host}"
        end
        if host.end_with?('.')
          raise Error::InvalidAddress, "Hostname cannot end with a dot: #{host}"
        end
        if host.include?('..')
          raise Error::InvalidAddress, "Runs of multiple dots are not allowed in hostname: #{host}"
        end
      end

      def validate_port_str!(port)
        unless port.nil? || (port.length > 0 && port.to_i > 0 && port.to_i <= 65535)
          raise Error::InvalidAddress,
            "Invalid port: #{port}. Port must be an integer greater than 0 and less than 65536"
        end
      end
    end
  end
end
