# Copyright (C) 2014-2017 MongoDB, Inc.
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

    # The SRVScheme URI class parses a MongoDB uri formatted as
    # defined in the Initial DNS Seedlist Discovery spec.
    #
    # https://github.com/mongodb/specifications/blob/master/source/initial-dns-seedlist-discovery
    #
    # @example Use the uri string to make a client connection.
    #   uri = URI.new('mongodb+srv://test6.test.build.10gen.cc/')
    #   client = Client.new(uri.server, uri.options)
    #   client.login(uri.credentials)
    #   client[uri.database]

    # Parser for a URI using the mongodb+srv protocol. This URI specifies a DNS to query for SRV records.
    # The driver will query the DNS server for SRV records on {hostname}.{domainname},
    # prefixed with _mongodb._tcp
    # The SRV records are then used as the seedlist for the Mongo::Client.
    # The driver also queries for TXT records providing default connection string options.
    #
    # @since 2.5.0
    class SRVScheme < URI

      # Gets the options hash that needs to be passed to a Mongo::Client on
      # instantiation, so we don't have to merge the credentials and database in
      # at that point - we only have a single point here.
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

      RECORD_PREFIX = '_mongodb._tcp.'

      VALID_TXT_OPTIONS = [:auth_source, :replica_set]

      def parse_creds_hosts!(string)
        hostname, creds = split_creds_hosts(string)
        validate_hostname!(hostname)
        records = get_records(hostname)
        @txt_options = get_txt_opts(hostname)
        @servers = parse_servers!(records.join(','))
        @user = parse_user!(creds)
        @password = parse_password!(creds)
      end

      def validate_hostname!(host)
        # verify no port
        # verify only one hostname
        raise Error::InvalidURI.new(host, INVALID_SCHEME) if host.include?(':')
        raise Error::InvalidURI.new(host, INVALID_SCHEME) if host.include?(',')
        # verify domain has two parts
        hostname, _, domain = host.partition('.')
        raise Error::InvalidURI.new(host, INVALID_SCHEME) unless domain.include?('.')
      end

      def get_records(hostname)
        name = RECORD_PREFIX + hostname
        records = resolver.getresources(name, Resolv::DNS::Resource::IN::SRV).collect do |record|
          host = record.target.to_s
          port = record.port
          validate_record!(host, hostname)
          "#{host}:#{port}"
          end
        raise Exception if records.empty?
        records
      end

      def validate_record!(host, domain)
        root = domain.split('.')[1..-1]
        host_parts = host.split('.')
        raise Exception unless host_parts.size > root.size && root == host_parts[-root.length..-1]
      end

      def get_txt_opts(host)
        records = resolver.getresources(host, Resolv::DNS::Resource::IN::TXT)
        unless records.empty?
          raise Exception if records.size > 1
          options_string = records[0].strings.join
          opts = parse_uri_options!(options_string)
          validate_txt_options!(opts)
          opts
        end || {}
      end

      def validate_txt_options!(opts)
        raise Exception unless opts.keys.all? { |key| VALID_TXT_OPTIONS.include?(key) }
      end

      def resolver
        @resolver ||= Resolv::DNS.new
      end
    end
  end
end
