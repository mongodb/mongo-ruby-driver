# Copyright (C) 2014-2019 MongoDB, Inc.
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

RSpec::Matchers.define :have_hosts do |test|

  match do |cl|

    def find_server(client, host)
      client.cluster.instance_variable_get(:@servers).detect do |s|
        s.address.host == host.host
      end
    end

    def match_host?(server, host)
      server.address.host == host.host
    end

    def match_port?(server, host)
      server.address.port == host.port || !host.port
    end

    def match_address_family?(server, host)
      address_family(server) == host.address_family
    end

    def address_family(server)
      server.address.socket(2)
      server.address.instance_variable_get(:@resolver).class
    end

    test.hosts.all? do |host|
      server = find_server(cl, host)
      match_host?(server, host) &&
          match_port?(server, host) if server #&&
          #match_address_family?(server, host) if server
    end

    failure_message do |client|
      "With URI: #{test.uri_string}\n" +
          "Expected that test hosts: #{test.hosts} would match " +
          "client hosts: #{cl.cluster.instance_variable_get(:@servers)}"
    end
  end
end

RSpec::Matchers.define :match_auth do |test|

  def match_database?(client, auth)
    client.options[:database] == auth.database || !auth.database
  end

  def match_password?(client, auth)
    client.options[:password] == auth.password ||
      client.options[:password].nil? && auth.password == ''
  end

  match do |client|
    auth = test.auth
    return true unless auth
    client.options[:user] == auth.username &&
      match_password?(client, auth) &&
        match_database?(client, auth)
  end

  failure_message do |client|
    "With URI: #{test.uri_string}\n" +
        "Expected that test auth: #{test.auth} would match client auth: #{client.options}"
  end
end

RSpec::Matchers.define :match_options do |test|

  match do |client|
    options = test.options
    return true unless options
    options.match?(client.options)
  end

  failure_message do |client|
    "With URI: #{test.uri_string}\n" +
      "Expected that test options: #{test.options.options} would match client options: #{client.options}"
  end
end

module Mongo
  module ConnectionString

    class Spec

      attr_reader :description

      # Instantiate the new spec.
      #
      # @example Create the spec.
      #   Spec.new(file)
      #
      # @param [ String ] file The name of the file.
      #
      # @since 2.0.0
      def initialize(file)
        file = File.new(file)
        @spec = YAML.load(ERB.new(file.read).result)
        file.close
        @description = File.basename(file)
      end

      def tests
        @tests ||= @spec['tests'].collect do |spec|
          Test.new(spec)
        end
      end
    end

    class Test

      attr_reader :description
      attr_reader :uri_string

      def initialize(spec)
        @spec = spec
        @description = @spec['description']
        @uri_string = @spec['uri']
      end

      def valid?
        @spec['valid']
      end

      def warn?
        @spec['warning']
      end

      def hosts
        @hosts ||= (@spec['hosts'] || []).collect do |host|
          Host.new(host)
        end
      end

      def options
        @options ||= Options.new(@spec['options']) if @spec['options']
      end

      def client
        @client ||= ClientRegistry.instance.new_local_client(@spec['uri'], monitoring_io: false)
      end

      def uri
        @uri ||= Mongo::URI.get(@spec['uri'])
      end

      def auth
        @auth ||= Auth.new(@spec['auth']) if @spec['auth']
      end

      def raise_error?
        @spec['error']
      end
    end

    class Host

      MAPPING = {
          'ipv4' => Mongo::Address::IPv4,
          'ipv6' => Mongo::Address::IPv6,
          'unix' => Mongo::Address::Unix
      }

      attr_reader :host
      attr_reader :port

      def initialize(spec)
        @spec = spec
        @host = @spec['host']
        @port = @spec['port']
      end

      def address_family
        MAPPING[@spec['type']]
      end
    end

    class Auth

      attr_reader :username
      attr_reader :password
      attr_reader :database

      def initialize(spec)
        @spec = spec
        @username = @spec['username']
        @password = @spec['password']
        @database = @spec['db']
      end

      def to_s
        "username: #{username}, password: #{password}, database: #{database}"
      end
    end

    class Options

      MAPPINGS = {
        # Replica Set Options
        'replicaset' => :replica_set,

        # Timeout Options
        'connecttimeoutms' => :connect_timeout,
        'sockettimeoutms' => :socket_timeout,
        'serverselectiontimeoutms' => :server_selection_timeout,
        'localthresholdms' => :local_threshold,
        'heartbeatfrequencyms' => :heartbeat_frequency,
        'maxidletimems' => :max_idle_time,

         # Write  Options
        'journal' => [:write, 'j'],
        'w' => [:write, 'w'],
        'wtimeoutms' => [:write, 'wtimeout'],

        # Read Options
        'readpreference' => ['read', 'mode'],
        'readpreferencetags' => ['read', 'tag_sets'],
        'maxstalenessseconds' => ['read', 'max_staleness'],

        # Pool Options
        'minpoolsize' => :min_pool_size,
        'maxpoolsize' => :max_pool_size,

        # Security Options
        'tls' => :ssl,
        'tlsallowinvalidcertificates' => :ssl_verify_certificate,
        'tlsallowinvalidhostnames' => :ssl_verify_hostname,
        'tlscafile' => :ssl_ca_cert,
        'tlscertificatekeyfile' => :ssl_cert,
        'tlscertificatekeyfilepassword' => :ssl_key_pass_phrase,
        'tlsinsecure' => :ssl_verify,

        # Auth Options
        'authsource' => :auth_source,
        'authmechanism' => :auth_mech,
        'authmechanismproperties' => :auth_mech_properties,

        # Client Options
        'appname' => :app_name,
        'readconcernlevel' => [:read_concern, 'level'],
        'retrywrites' => :retry_writes,
        'zlibcompressionlevel' => :zlib_compression_level,
      }

      attr_reader :options

      def initialize(options)
        @options = options
      end

      def match?(opts)
        @options.all? do |k, v|
          k = k.downcase

          expected =
            case k
            when 'authmechanism'
              Mongo::URI::AUTH_MECH_MAP[v].downcase.to_s
            when 'authsource'
              v == '$external' ? 'external' : v.downcase
            when 'authmechanismproperties'
              v.reduce({}) do |new_v, prop|
                prop_key = prop.first.downcase
                prop_val = prop.last == 'true' ? true : prop.last
                new_v[prop_key] = prop_val

                new_v
              end
            when 'compressors'
              v.dup.tap do |compressors|
                # The Ruby driver doesn't support snappy
                compressors.delete('snappy')
              end
            when 'readpreference'
              Mongo::URI::READ_MODE_MAP[v.downcase].to_s
            when 'tlsallowinvalidcertificates', 'tlsallowinvalidhostnames', 'tlsinsecure'
              !v
            else
              if k.end_with?('ms') && k != 'wtimeoutms'
                v / 1000.0
              elsif v.is_a?(String)
                v.downcase
              else
                v
              end
            end

          actual =
            case MAPPINGS[k]
            when nil
              opts[k]
            when Array
              opts[MAPPINGS[k].first][MAPPINGS[k].last]
            else
              opts[MAPPINGS[k]]
            end

          actual = actual.to_s if actual.is_a?(Symbol)
          actual.downcase! if actual.is_a?(String)

          expected == actual
        end
      end
    end
  end
end
