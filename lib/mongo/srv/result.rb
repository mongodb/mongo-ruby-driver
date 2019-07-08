# Copyright (C) 2017-2019 MongoDB, Inc.
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

  module Srv

    # SRV record lookup result.
    #
    # Contains server addresses that the query resolved to, and minimum TTL
    # of the DNS records.
    #
    # @api private
    class Result

      # @return [ String ] MISMATCHED_DOMAINNAME Error message format string indicating that an SRV
      #   record found does not match the domain of a hostname.
      MISMATCHED_DOMAINNAME = "Parent domain name in SRV record result (%s) does not match " +
                                 "that of the hostname (%s)".freeze

      # @return [ String ] query_hostname The hostname pointing to the DNS records.
      attr_reader :query_hostname

      # @return [ Array<String> ] address_strs The host strings of the SRV records
      #   for the query hostname.
      attr_reader :address_strs

      # @return [ Integer | nil ] min_ttl The smallest TTL found among the
      #   records (or nil if no records have been added).
      attr_accessor :min_ttl

      # Create a new object to keep track of the SRV records of the hostname.
      #
      # @param [ String ] hostname The hostname pointing to the DNS records.
      def initialize(hostname)
        @query_hostname = hostname
        @address_strs = []
        @min_ttl = nil
      end

      # Checks whether there are any records.
      #
      # @return [ Boolean ] Whether or not there are any records.
      def empty?
        @address_strs.empty?
      end

      # Adds a new record.
      #
      # @param [ Resolv::DNS::Resource ] record An SRV record found for the hostname.
      def add_record(record)
        record_host = record.target.to_s
        port = record.port
        validate_record!(record_host)
        address_str = if record_host.index(':')
          # IPV6 address
          "[#{record_host}]:#{port}"
        else
          "#{record_host}:#{port}"
        end
        @address_strs << address_str

        if @min_ttl.nil?
          @min_ttl = record.ttl
        else
          @min_ttl = [@min_ttl, record.ttl].min
        end

        nil
      end

      private

      # Ensures that a record's domain name matches that of the hostname.
      #
      # A hostname's domain name consists of each of the '.' delineated
      # parts after the first. For example, the hostname 'foo.bar.baz'
      # has the domain name 'bar.baz'.
      #
      # @param [ String ] record_host The host of the SRV record.
      #
      # @raise [ Mongo::Error::MismatchedDomain ] If the record's domain name doesn't match that of
      #   the hostname.
      def validate_record!(record_host)
        @domainname ||= query_hostname.split(URI::SRVProtocol::DOT_PARTITION)[1..-1]
        host_parts = record_host.split(URI::SRVProtocol::DOT_PARTITION)

        unless (host_parts.size > @domainname.size) && (@domainname == host_parts[-@domainname.length..-1])
          raise Error::MismatchedDomain.new(MISMATCHED_DOMAINNAME % [record_host, @domainname])
        end
      end
    end
  end
end
