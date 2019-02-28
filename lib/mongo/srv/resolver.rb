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

    # The SRV::Resolver class encapsulates the necessary behavior for querying SRV records as
    # required by the driver.
    #
    # @api private
    class Resolver
      include Loggable

      # @return [ String ] RECORD_PREFIX The prefix appended to each hostname before querying SRV
      #   records.
      RECORD_PREFIX = '_mongodb._tcp.'.freeze

      # Creates a new Resolver.
      #
      # @param [ Hash ] options The options for the resolver.
      #
      # @option options [ Boolean ] :raise_on_invalid Whether or not to raise an error if either a
      #   record with a mismatched domain is found or if no records are found. Defaults to true.
      def initialize(options = nil)
        @options ||= {}
        @resolver = Resolv::DNS.new
      end

      # Obtains all of the SRV records for a given hostname. If a record with a mismatched domain is
      # found or no records a found, then an error will be raised if the :raise_on_invalid Resolver
      # option is true and a warning will be logged otherwise.
      #
      # @param [ String ] hostname The hostname whose records should be obtained.
      #
      # @raise [ Mongo::Error::MismatchedDomain ] If the :raise_in_invalid Resolver option is true
      #   and a record with a domain name that does not match the hostname's is found.
      # @raise [ Mongo::Error::NoSRVRecords ] If the :raise_in_invalid Resolver option is true
      #   and a no records are found.
      #
      # @return [ Mongo::SRV::Records ] The records obtained for the hostname.
      def get_records(hostname)
        query_name = RECORD_PREFIX + hostname
        resources = @resolver.getresources(query_name, Resolv::DNS::Resource::IN::SRV)

        # Collect all of the records into a Records object, raising an error or logging a warning if
        # a record with a mismatched domain is found. Note that in the case a warning is raised, the
        # record is _not_ added to the Records object.
        records = resources.reduce(SRV::Records.new(hostname)) do |records, record|
          begin
            records.add_record(record)
          rescue Error::MismatchedDomain => e
            if raise_on_invalid?
              raise
            else
              log_warn(e.message)
            end
          end

          records
        end

        # If no records are found, either raise an error or log a warning based on the Resolver's
        # :raise_on_invalid option.
        if records.empty?
          if raise_on_invalid?
            raise Error::NoSRVRecords.new(URI::SRVProtocol::NO_SRV_RECORDS % hostname)
          else
            log_warn(URI::SRVProtocol::NO_SRV_RECORDS % hostname)
          end
        end

        records
      end

      # Obtains the TXT records of a host.
      #
      # @param [ String ] host The host whose TXT records should be obtained.
      #
      # @return [ Array<Resolv::DNS::Resource> ] The TXT records.
      def get_txt_opts(host)
        @resolver.getresources(host, Resolv::DNS::Resource::IN::TXT)
      end

      private

      # Checks whether an error should be raised due to either a record with a mismatched domain
      # being found or no records being found.
      #
      # @return [ Boolean ] Whether an error should be raised.
      def raise_on_invalid?
        @raise_on_invalid ||= options[:raise_on_invalid] || true
      end
    end
  end
end
