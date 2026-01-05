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
  module Srv

    # Encapsulates the necessary behavior for querying SRV records as
    # required by the driver.
    #
    # @api private
    class Resolver
      include Loggable

      # @return [ String ] RECORD_PREFIX The prefix prepended to each hostname
      #   before querying SRV records.
      RECORD_PREFIX = '_mongodb._tcp.'.freeze

      # Generates the record prefix with a custom SRV service name if it is
      # provided.
      #
      # @option srv_service_name [ String | nil ] The SRV service name to use
      #   in the record prefix.
      # @return [ String ] The generated record prefix.
      def record_prefix(srv_service_name=nil)
        return srv_service_name ? "_#{srv_service_name}._tcp." : RECORD_PREFIX
      end

      # Creates a new Resolver.
      #
      # @option opts [ Float ] :timeout The timeout, in seconds, to use for
      #   each DNS record resolution.
      # @option opts [ Boolean ] :raise_on_invalid Whether or not to raise
      #   an exception if either a record with a mismatched domain is found
      #   or if no records are found. Defaults to true.
      # @option opts [ Hash ] :resolv_options For internal driver use only.
      #   Options to pass through to Resolv::DNS constructor for SRV lookups.
      def initialize(**opts)
        @options = opts.freeze
        @resolver = Resolv::DNS.new(@options[:resolv_options])
        @resolver.timeouts = timeout
      end

      # @return [ Hash ] Resolver options.
      attr_reader :options

      def timeout
        options[:timeout] || Monitor::DEFAULT_TIMEOUT
      end

      # Obtains all of the SRV records for a given hostname. If a srv_max_hosts
      # is specified and it is greater than 0, return maximum srv_max_hosts records.
      #
      # In the event that a record with a mismatched domain is found or no
      # records are found, if the :raise_on_invalid option is true,
      # an exception will be raised, otherwise a warning will be logged.
      #
      # @param [ String ] hostname The hostname whose records should be obtained.
      # @param [ String | nil ] srv_service_name The SRV service name for the DNS query.
      #   If nil, 'mongodb' is used.
      # @param [ Integer | nil ] srv_max_hosts The maximum number of records to return.
      #   If this value is nil, return all of the records.
      #
      # @raise [ Mongo::Error::MismatchedDomain ] If the :raise_in_invalid
      #   Resolver option is true and a record with a domain name that does
      #   not match the hostname's is found.
      # @raise [ Mongo::Error::NoSRVRecords ] If the :raise_in_invalid Resolver
      #   option is true and no records are found.
      #
      # @return [ Mongo::Srv::Result ] SRV lookup result.
      def get_records(hostname, srv_service_name=nil, srv_max_hosts=nil)
        query_name = record_prefix(srv_service_name) + hostname
        resources = @resolver.getresources(query_name, Resolv::DNS::Resource::IN::SRV)

        # Collect all of the records into a Result object, raising an error
        # or logging a warning if a record with a mismatched domain is found.
        # Note that in the case a warning is raised, the record is _not_
        # added to the Result object.
        result = Srv::Result.new(hostname)
        resources.each do |record|
          begin
            result.add_record(record)
          rescue Error::MismatchedDomain => e
            if raise_on_invalid?
              raise
            else
              log_warn(e.message)
            end
          end
        end

        # If no records are found, either raise an error or log a warning
        # based on the Resolver's :raise_on_invalid option.
        if result.empty?
          if raise_on_invalid?
            raise Error::NoSRVRecords.new(URI::SRVProtocol::NO_SRV_RECORDS % hostname)
          else
            log_warn(URI::SRVProtocol::NO_SRV_RECORDS % hostname)
          end
        end

        # if srv_max_hosts is in [1, #addresses)
        if (1...result.address_strs.length).include? srv_max_hosts
          sampled_records = resources.shuffle.first(srv_max_hosts)
          result = Srv::Result.new(hostname)
          sampled_records.each { |record| result.add_record(record) }
        end
        result
      end

      # Obtains the TXT records of a host.
      #
      # @param [ String ] hostname The host whose TXT records should be obtained.
      #
      # @return [ nil | String ] URI options string from TXT record
      #   associated with the hostname, or nil if there is no such record.
      #
      # @raise [ Mongo::Error::InvalidTXTRecord ] If more than one TXT record is found.
      def get_txt_options_string(hostname)
        records = @resolver.getresources(hostname, Resolv::DNS::Resource::IN::TXT)
        if records.empty?
          return nil
        end

        if records.length > 1
          msg = "Only one TXT record is allowed: querying hostname #{hostname} returned #{records.length} records"

          raise Error::InvalidTXTRecord, msg
        end

        records[0].strings.join
      end

      private

      # Checks whether an error should be raised due to either a record with
      # a mismatched domain being found or no records being found.
      #
      # @return [ Boolean ] Whether an error should be raised.
      def raise_on_invalid?
        @raise_on_invalid ||= @options[:raise_on_invalid] || true
      end
    end
  end
end
