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

require 'resolv'

module Mongo

  class URI

    # Parser for a URI using the mongodb+srv protocol, which specifies a DNS to query for SRV records.
    # The driver will query the DNS server for SRV records on {hostname}.{domainname},
    # prefixed with _mongodb._tcp
    # The SRV records can then be used as the seedlist for a Mongo::Client.
    # The driver also queries for a TXT record providing default connection string options.
    # Only one TXT record is allowed, and only a subset of Mongo::Client options is allowed.
    #
    # Please refer to the Initial DNS Seedlist Discovery spec for details.
    #
    # https://github.com/mongodb/specifications/blob/master/source/initial-dns-seedlist-discovery
    #
    # @example Use the uri string to make a client connection.
    #   client = Mongo::Client.new('mongodb+srv://test6.test.build.10gen.cc/')
    #
    # @since 2.5.0
    class SRVProtocol < URI

      # Gets the options hash that needs to be passed to a Mongo::Client on instantiation, so we
      # don't have to merge the txt record options, credentials, and database in at that point -
      # we only have a single point here.
      #
      # @example Get the client options.
      #   uri.client_options
      #
      # @return [ Hash ] The options passed to the Mongo::Client
      #
      # @since 2.5.0
      def client_options
        opts = @txt_options.merge(ssl: true)
        opts = opts.merge(uri_options).merge(:database => database)
        @user ? opts.merge(credentials) : opts
      end

      private

      RECORD_PREFIX = '_mongodb._tcp.'.freeze

      DOT_PARTITION = '.'.freeze

      VALID_TXT_OPTIONS = ['replicaset', 'authsource'].freeze

      INVALID_HOST = "One and only one host is required in a connection string with the " +
                       "'#{MONGODB_SRV_SCHEME}' protocol.".freeze

      INVALID_PORT = "It is not allowed to specify a port in a connection string with the " +
                       "'#{MONGODB_SRV_SCHEME}' protocol.".freeze

      INVALID_DOMAIN = "The domain name must consist of at least two parts: the domain name, " +
                         "and a TLD.".freeze

      NO_SRV_RECORDS = "The DNS query returned no SRV records at hostname (%s)".freeze

      MORE_THAN_ONE_TXT_RECORD_FOUND = "Only one TXT record is allowed. Querying hostname (%s) " +
                                         "returned more than one result.".freeze

      INVALID_TXT_RECORD_OPTION = "TXT records can only specify the options " +
                                    "[#{VALID_TXT_OPTIONS.join(', ')}].".freeze

      MISMATCHED_DOMAINNAME = "Parent domain name in SRV record result (%s) does not match " +
                                 "that of the hostname (%s)".freeze

      FORMAT = 'mongodb+srv://[username:password@]host[/[database][?options]]'.freeze

      def scheme
        MONGODB_SRV_SCHEME
      end

      def raise_invalid_error!(details)
        raise Error::InvalidURI.new(@string, details, FORMAT)
      end

      def resolver
        @resolver ||= Resolv::DNS.new
      end

      def parse_creds_hosts!(string)
        hostname, creds = split_creds_hosts(string)
        validate_hostname!(hostname)
        records = get_records(hostname)
        @txt_options = get_txt_opts(hostname) || {}
        @servers = parse_servers!(records.join(','))
        @user = parse_user!(creds)
        @password = parse_password!(creds)
      end

      # Validates that a hostname fits the expected format.
      #
      # @param [ String ] hostname The hostname to validate.
      #
      # @raise [ Mongo::Error::InvalidURI ] If any of the following conditions are true:
      #   * the hostname is empty
      #   * the hostname contains either ',' or ':'
      #   * the hostname begins with '.'
      #   * the hostname contains consecutive '.' characters
      def validate_hostname!(hostname)
        raise_invalid_error!(INVALID_HOST) if hostname.empty?
        raise_invalid_error!(INVALID_HOST) if hostname.include?(HOST_DELIM)
        raise_invalid_error!(INVALID_PORT) if hostname.include?(HOST_PORT_DELIM)
        raise_invalid_error!(INVALID_HOST) if hostname.start_with?(DOT_PARTITION)
        raise_invalid_error!(INVALID_HOST) if hostname.end_with?('..')

        parts = hostname.split(DOT_PARTITION)
        raise_invalid_error!(INVALID_HOST) if parts.any?(&:empty?)
        raise_invalid_error!(INVALID_HOST) if parts.count < 3
      end

      def get_records(hostname)
        query_name = RECORD_PREFIX + hostname
        records = resolver.getresources(query_name, Resolv::DNS::Resource::IN::SRV).collect do |record|
          record_host = record.target.to_s
          port = record.port
          validate_record!(record_host, hostname)
          "#{record_host}#{HOST_PORT_DELIM}#{port}"
        end
        raise Error::NoSRVRecords.new(NO_SRV_RECORDS % hostname) if records.empty?
        records
      end

      def validate_record!(record_host, hostname)
        domainname = hostname.split(DOT_PARTITION)[1..-1]
        host_parts = record_host.split(DOT_PARTITION)
        unless (host_parts.size > domainname.size) && (domainname == host_parts[-domainname.length..-1])
          raise Error::MismatchedDomain.new(MISMATCHED_DOMAINNAME % [record_host, domainname])
        end
      end

      def get_txt_opts(host)
        records = resolver.getresources(host, Resolv::DNS::Resource::IN::TXT)
        unless records.empty?
          if records.size > 1
            raise Error::InvalidTXTRecord.new(MORE_THAN_ONE_TXT_RECORD_FOUND % host)
          end
          options_string = records[0].strings.join
          parse_txt_options!(options_string)
        end
      end

      def parse_txt_options!(string)
        return {} unless string
        string.split(INDIV_URI_OPTS_DELIM).reduce({}) do |txt_options, opt|
          raise Error::InvalidTXTRecord.new(INVALID_OPTS_VALUE_DELIM) unless opt.index(URI_OPTS_VALUE_DELIM)
          key, value = opt.split(URI_OPTS_VALUE_DELIM)
          raise Error::InvalidTXTRecord.new(INVALID_TXT_RECORD_OPTION) unless VALID_TXT_OPTIONS.include?(key.downcase)
          strategy = URI_OPTION_MAP[key.downcase]
          add_uri_option(strategy, value, txt_options)
          txt_options
        end
      end
    end
  end
end
