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

  class URI

    # Parser for a URI using the mongodb+srv protocol, which specifies a DNS to query for SRV records.
    # The driver will query the DNS server for SRV records on <hostname>.<domainname>,
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

      attr_reader :srv_records

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

      # @return [ Srv::Result ] SRV lookup result.
      #
      # @api private
      attr_reader :srv_result

      # The hostname that is specified in the URI and used to look up
      # SRV records.
      #
      # This attribute needs to be defined because SRVProtocol changes
      # #servers to be the result of the lookup rather than the hostname
      # specified in the URI.
      #
      # @return [ String ] The hostname used in SRV lookup.
      #
      # @api private
      attr_reader :query_hostname

      private

      # @return [ String ] DOT_PARTITION The '.' character used to delineate the parts of a
      #   hostname.
      #
      # @deprecated
      DOT_PARTITION = '.'.freeze

      # @return [ Array<String> ] VALID_TXT_OPTIONS The valid options for a TXT record to specify.
      VALID_TXT_OPTIONS = %w(replicaset authsource loadbalanced).freeze

      # @return [ String ] INVALID_HOST Error message format string indicating that the hostname in
      #   in the URI does not fit the expected form.
      INVALID_HOST = "One and only one host is required in a connection string with the " +
                       "'#{MONGODB_SRV_SCHEME}' protocol.".freeze

      # @return [ String ] INVALID_PORT Error message format string indicating that a port was
      #   included with an SRV hostname.
      INVALID_PORT = "It is not allowed to specify a port in a connection string with the " +
                       "'#{MONGODB_SRV_SCHEME}' protocol.".freeze

      # @return [ String ] INVALID_DOMAIN Error message format string indicating that the domain name
      #   of the hostname does not fit the expected form.
      # @deprecated
      INVALID_DOMAIN = "The domain name must consist of at least two parts: the domain name, " +
                         "and a TLD.".freeze

      # @return [ String ] NO_SRV_RECORDS Error message format string indicating that no SRV records
      #   were found.
      NO_SRV_RECORDS = "The DNS query returned no SRV records for '%s'".freeze

      # @return [ String ] FORMAT The expected SRV URI format.
      FORMAT = 'mongodb+srv://[username:password@]host[/[database][?options]]'.freeze

      # Gets the MongoDB SRV URI scheme.
      #
      # @return [ String ] The MongoDB SRV URI scheme.
      def scheme
        MONGODB_SRV_SCHEME
      end

      # Raises an InvalidURI error.
      #
      # @param [ String ] details A detailed error message.
      #
      # @raise [ Mongo::Error::InvalidURI ]
      def raise_invalid_error!(details)
        raise Error::InvalidURI.new(@string, details, FORMAT)
      end

      # Gets the SRV resolver.
      #
      # @return [ Mongo::Srv::Resolver ]
      def resolver
        @resolver ||= Srv::Resolver.new(
          raise_on_invalid: true,
          resolv_options: options[:resolv_options],
          timeout: options[:connect_timeout],
        )
      end

      # Parses the credentials from the URI and performs DNS queries to obtain
      # the hosts and TXT options.
      #
      # @param [ String ] remaining The portion of the URI pertaining to the
      #   authentication credentials and the hosts.
      def parse!(remaining)
        super

        if @servers.length != 1
          raise_invalid_error!(INVALID_HOST)
        end
        hostname = @servers.first
        validate_srv_hostname(hostname)
        @query_hostname = hostname

        @srv_result = resolver.get_records(hostname, uri_options[:srv_service_name], uri_options[:srv_max_hosts])
        if srv_result.empty?
          raise Error::NoSRVRecords.new(NO_SRV_RECORDS % hostname)
        end
        @txt_options = get_txt_options(hostname) || {}
        records = srv_result.address_strs
        records.each do |record|
          validate_address_str!(record)
        end
        @servers = records
      rescue Error::InvalidAddress => e
        raise_invalid_error!(e.message)
      end

      # Validates the hostname used in an SRV URI.
      #
      # The hostname cannot include a port.
      #
      # The hostname must not begin with a dot, end with a dot, or have
      # consecutive dots. The hostname must have a minimum of 3 total
      # components (foo.bar.tld).
      #
      # Raises Error::InvalidURI if validation fails.
      def validate_srv_hostname(hostname)
        raise_invalid_error!(INVALID_PORT) if hostname.include?(HOST_PORT_DELIM)

        if hostname.start_with?('.')
          raise_invalid_error!("Hostname cannot start with a dot: #{hostname}")
        end
        if hostname.end_with?('.')
          raise_invalid_error!("Hostname cannot end with a dot: #{hostname}")
        end
        parts = hostname.split('.')
        if parts.any?(&:empty?)
          raise_invalid_error!("Hostname cannot have consecutive dots: #{hostname}")
        end
        if parts.length < 3
          raise_invalid_error!("Hostname must have a minimum of 3 components (foo.bar.tld): #{hostname}")
        end
      end

      # Obtains the TXT options of a host.
      #
      # @param [ String ] hostname The hostname whose records should be obtained.
      #
      # @return [ Hash ] The TXT record options (an empyt hash if no TXT
      #   records are found).
      #
      # @raise [ Mongo::Error::InvalidTXTRecord ] If more than one TXT record is found.
      def get_txt_options(hostname)
        options_string = resolver.get_txt_options_string(hostname)
        if options_string
          parse_txt_options!(options_string)
        else
          {}
        end
      end

      # Parses the TXT record options into a hash and adds the options to set of all URI options
      # parsed.
      #
      # @param [ String ] string The concatenated TXT options.
      #
      # @return [ Hash ] The parsed TXT options.
      #
      # @raise [ Mongo::Error::InvalidTXTRecord ] If the TXT record does not fit the expected form
      #   or the option specified is not a valid TXT option.
      def parse_txt_options!(string)
        string.split(INDIV_URI_OPTS_DELIM).reduce({}) do |txt_options, opt|
          raise Error::InvalidTXTRecord.new(INVALID_OPTS_VALUE_DELIM) unless opt.index(URI_OPTS_VALUE_DELIM)
          key, value = opt.split('=')
          unless VALID_TXT_OPTIONS.include?(key.downcase)
            msg = "TXT records can only specify the options [#{VALID_TXT_OPTIONS.join(', ')}]: #{string}"
            raise Error::InvalidTXTRecord.new(msg)
          end
          options_mapper.add_uri_option(key, value, txt_options)
          txt_options
        end
      end

      def validate_uri_options!
        if uri_options[:direct_connection]
          raise_invalid_error_no_fmt!("directConnection=true is incompatible with SRV URIs")
        end

        super
      end
    end
  end
end
