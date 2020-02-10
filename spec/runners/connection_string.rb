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

RSpec::Matchers.define :have_hosts do |test, hosts|

  match do |cl|

    def find_server(client, host)
      client.cluster.servers_list.detect do |s|
        if host.port
          s.address.host == host.host && s.address.port == host.port
        else
          s.address.host == host.host
        end
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

    hosts.all? do |host|
      server = find_server(cl, host)
      server &&
        match_host?(server, host) &&
        match_port?(server, host) #&&
        #match_address_family?(server, host)
    end
  end

  failure_message do |client|
    "With URI: #{test.uri_string}\n" +
        "Expected client hosts: #{client.cluster.instance_variable_get(:@servers)} " +
        "to match #{hosts}"
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
      # @param [ String ] test_path The path to the file.
      #
      # @since 2.0.0
      def initialize(test_path)
        @spec = YAML.load(File.read(test_path))
        @description = File.basename(test_path)
      end

      def tests
        @tests ||= @spec['tests'].collect do |spec|
          Test.new(spec)
        end
      end
    end

    class Test
      include RSpec::Core::Pending

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

      def seeds
        if @spec['seeds']
          @seeds ||= (@spec['seeds'] || []).collect do |host|
            Host.new(host)
          end
        else
          nil
        end
      end

      def options
        @options ||= Options.new(@spec['options']) if @spec['options']
      end

      def client
        @client ||= ClientRegistry.instance.new_local_client(@spec['uri'], monitoring_io: false)
      rescue Mongo::Error::LintError => e
        if e.message =~ /arbitraryButStillValid/
          skip 'Test uses a read concern that fails linter'
        end
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

      def read_concern_expectation
        @spec['readConcern']
      end

      def write_concern_expectation
        @spec['writeConcern']
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
        if spec.is_a?(Hash)
          # Connection string spec tests
          @spec = spec
          @host = @spec['host']
          @port = @spec['port']
        else
          # DNS seed list spec tests
          address = Mongo::Address.new(spec)
          @host = address.host
          @port = address.port
        end
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
        'journal' => [:write_concern, 'j'],
        'w' => [:write_concern, 'w'],
        'wtimeoutms' => [:write_concern, 'wtimeout'],

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

          if actual.is_a?(Symbol)
            actual = actual.to_s
          end
          if actual.is_a?(String)
            actual = actual.downcase
          end

          expected == actual
        end
      end
    end
  end
end

def define_connection_string_spec_tests(test_paths, spec_cls = Mongo::ConnectionString::Spec, &block)

  clean_slate_for_all_if_possible

  test_paths.each do |path|

    spec = spec_cls.new(path)

    context(spec.description) do

      #include Mongo::ConnectionString

      spec.tests.each_with_index do |test, index|
        context "when a #{test.description} is provided" do
          if test.description.downcase.include?("gssapi")
            require_mongo_kerberos
          end

          context 'when the uri is invalid', unless: test.valid? do

            it 'raises an error' do
              expect do
                test.uri
              end.to raise_exception(Mongo::Error::InvalidURI)
            end
          end

          context 'when the uri should warn', if: test.warn? do

            before do
              expect(Mongo::Logger.logger).to receive(:warn)
            end

            it 'warns' do
              expect(test.client).to be_a(Mongo::Client)
            end
          end

          context 'when the uri is valid', if: test.valid? do

            it 'does not raise an exception' do
              expect(test.uri).to be_a(Mongo::URI)
            end

            it 'creates a client with the correct hosts' do
              expect(test.client).to have_hosts(test, test.hosts)
            end

            it 'creates a client with the correct authentication options' do
              expect(test.client).to match_auth(test)
            end

            it 'creates a client with the correct options' do
              expect(test.client).to match_options(test)
            end

            if test.read_concern_expectation
              # Tests do not specify a read concern in the input and expect
              # the read concern to be {); our non-specified read concern is nil.
              # (But if a test used nil for the expectation, we wouldn't assert
              # read concern at all.)
              if test.read_concern_expectation == {}
                it 'creates a client with no read concern' do
                  actual = Utils.camelize_hash(test.client.options[:read_concern])
                  expect(actual).to be nil
                end
              else

                it 'creates a client with the correct read concern' do
                  actual = Utils.camelize_hash(test.client.options[:read_concern])
                  expect(actual).to eq(test.read_concern_expectation)
                end
              end
            end

            if test.write_concern_expectation
              let(:actual_write_concern) do
                Utils.camelize_hash(test.client.options[:write_concern])
              end

              let(:expected_write_concern) do
                test.write_concern_expectation.dup.tap do |expected|
                  # Spec tests have expectations on the "driver API" which is
                  # different from what is being sent to the server. In Ruby
                  # the "driver API" matches what we send to the server, thus
                  # these expectations are rather awkward to work with.
                  # Convert them all to expected server fields.
                  j = expected.delete('journal')
                  unless j.nil?
                    expected['j'] = j
                  end
                  wtimeout = expected.delete('wtimeoutMS')
                  unless wtimeout.nil?
                    expected['wtimeout'] = wtimeout
                  end
                end
              end

              if test.write_concern_expectation == {}

                it 'creates a client with no write concern' do
                  expect(actual_write_concern).to be nil
                end
              else
                it 'creates a client with the correct write concern' do
                  expect(actual_write_concern).to eq(expected_write_concern)
                end
              end
            end
          end
        end
      end
    end
  end
end
