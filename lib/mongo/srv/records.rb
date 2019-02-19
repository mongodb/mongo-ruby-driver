# Copyright (C) 2014-2019 MongoDB, Inc.
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

  module SRV

    # This SRV::Records class is used to keep track of the SRV records discovered for a given hostname. It also keeps
    # track of the minimum TTL of the records added so far.
    #
    # @api private
    class Records

      MISMATCHED_DOMAINNAME = "Parent domain name in SRV record result (%s) does not match " +
                                 "that of the hostname (%s)".freeze

      attr_reader :hostname

      attr_reader :hosts

      attr_accessor :min_ttl

      def initialize(hostname)
        @hostname = hostname
        @hosts = []
        @min_ttl = nil
      end

      def empty?
        @hosts.empty?
      end

      def add_record(record)
        record_host = record.target.to_s
        port = record.port
        validate_record!(record_host, hostname)
        @hosts << "#{record_host}#{URI::SRVProtocol::HOST_PORT_DELIM}#{port}"

        if @min_ttl.nil?
          @min_ttl = record.ttl
        else
          @min_ttl = [@min_ttl, record.ttl].min
        end

        self
      end

      private

      def validate_record!(record_host, hostname)
        domainname = hostname.split(URI::SRVProtocol::DOT_PARTITION)[1..-1]
        host_parts = record_host.split(URI::SRVProtocol::DOT_PARTITION)
        unless (host_parts.size > domainname.size) && (domainname == host_parts[-domainname.length..-1])
          raise Error::MismatchedDomain.new(URI::SRVProtocol::MISMATCHED_DOMAINNAME % [record_host, domainname])
        end
      end
    end
  end
end
