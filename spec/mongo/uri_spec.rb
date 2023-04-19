# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::URI do

  shared_examples "roundtrips string" do
    it "returns the correct string for the uri" do
      expect(uri.to_s).to eq(URI::DEFAULT_PARSER.unescape(string))
    end
  end

  describe '.get' do

    let(:uri) { described_class.get(string) }

    describe 'invalid uris' do

      context 'string is not uri' do

        let(:string) { 'tyler' }

        it 'raises an error' do
          expect { uri }.to raise_error(Mongo::Error::InvalidURI)
        end
      end

      context 'nil' do

        let(:string) { nil }

        it 'raises an error' do
          expect do
            uri
          end.to raise_error(Mongo::Error::InvalidURI, /URI must be a string, not nil/)
        end
      end

      context 'empty string' do

        let(:string) { '' }

        it 'raises an error' do
          expect do
            uri
          end.to raise_error(Mongo::Error::InvalidURI, /Cannot parse an empty URI/)
        end
      end
    end

    context 'when the scheme is mongodb://' do

      let(:string) do
        'mongodb://localhost:27017'
      end

      it 'returns a Mongo::URI object' do
        expect(uri).to be_a(Mongo::URI)
      end
    end

    context 'when the scheme is mongodb+srv://' do
      require_external_connectivity

      let(:string) do
        'mongodb+srv://test5.test.build.10gen.cc'
      end

      it 'returns a Mongo::URI::SRVProtocol object' do
        expect(uri).to be_a(Mongo::URI::SRVProtocol)
      end

      include_examples "roundtrips string"
    end

    context 'when the scheme is invalid' do

      let(:string) do
        'mongo://localhost:27017'
      end

      it 'raises an exception' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end
  end

  let(:scheme) { 'mongodb://' }
  let(:uri) { described_class.new(string) }

  describe 'invalid uris' do

    context 'string is not uri' do

      let(:string) { 'tyler' }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'nil' do

      let(:string) { nil }

      it 'raises an error' do
        expect do
          uri
        end.to raise_error(Mongo::Error::InvalidURI, /URI must be a string, not nil/)
      end
    end

    context 'empty string' do

      let(:string) { '' }

      it 'raises an error' do
        expect do
          uri
        end.to raise_error(Mongo::Error::InvalidURI, /Cannot parse an empty URI/)
      end
    end

    context 'mongo://localhost:27017' do

      let(:string) { 'mongo://localhost:27017' }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb://' do

      let(:string) { 'mongodb://' }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb://localhost::27017' do

      let(:string) { 'mongodb://localhost::27017' }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb://localhost::27017/' do

      let(:string) { 'mongodb://localhost::27017/' }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb://::' do

      let(:string) { 'mongodb://::' }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb://localhost,localhost::' do

      let(:string) { 'mongodb://localhost,localhost::' }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb://localhost::27017,abc' do

      let(:string) { 'mongodb://localhost::27017,abc' }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb://localhost:-1' do

      let(:string) { 'mongodb://localhost:-1' }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb://localhost:0/' do

      let(:string) { 'mongodb://localhost:0/' }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb://localhost:65536' do

      let(:string) { 'mongodb://localhost:65536' }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb://localhost:foo' do

      let(:string) { 'mongodb://localhost:foo' }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb://[::1]:-1' do

      let(:string) { 'mongodb://[::1]:-1' }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb://[::1]:0/' do

      let(:string) { 'mongodb://[::1]:0/' }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb://[::1]:65536' do

      let(:string) { 'mongodb://[::1]:65536' }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb://[::1]:65536/' do

      let(:string) { 'mongodb://[::1]:65536/' }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb://[::1]:foo' do

      let(:string) { 'mongodb://[::1]:foo' }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'no slash after hosts, and options' do

      let(:string) { 'mongodb://example.com?tls=true' }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI, %r,MongoDB URI must have a slash \(/\) after the hosts if options are given,)
      end
    end

    context 'mongodb://example.com/?w' do

      let(:string) { 'mongodb://example.com/?w' }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI, /Option w has no value/)
      end
    end

    context 'equal sign in option value' do

      let(:string) { 'mongodb://example.com/?authmechanismproperties=foo:a=b&appname=test' }

      it 'is allowed' do
        expect(uri.uri_options[:auth_mech_properties]).to eq('foo' => 'a=b')
      end
    end

    context 'slash in option value' do

      let(:string) { 'mongodb://example.com/?tlsCAFile=a/b' }

      it 'returns a Mongo::URI object' do
        expect(uri).to be_a(Mongo::URI)
      end

      it 'parses correctly' do
        expect(uri.servers).to eq(['example.com'])
        expect(uri.uri_options[:ssl_ca_cert]).to eq('a/b')
      end
    end

    context 'numeric value in a string option' do

      let(:string) { 'mongodb://example.com/?appName=1' }

      it 'returns a Mongo::URI object' do
        expect(uri).to be_a(Mongo::URI)
      end

      it 'sets option to the string value' do
        expect(uri.uri_options[:app_name]).to eq('1')
      end
    end

    context 'options start with ampersand' do

      let(:string) { 'mongodb://example.com/?&appName=foo' }

      it 'returns a Mongo::URI object' do
        expect(uri).to be_a(Mongo::URI)
      end

      it 'parses the options' do
        expect(uri.uri_options[:app_name]).to eq('foo')
      end
    end

    context 'mongodb://alice:foo:bar@127.0.0.1' do

      let(:string) { 'mongodb://alice:foo:bar@127.0.0.1' }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb://alice@@127.0.0.1' do

      let(:string) { 'mongodb://alice@@127.0.0.1' }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb://alice@foo:bar@127.0.0.1' do

      let(:string) { 'mongodb://alice@foo:bar@127.0.0.1' }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

  end

  describe '#initialize' do
    context 'string is not uri' do
      let(:string) { 'tyler' }
      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end
  end

  describe "#to_s" do
    context "string is a uri" do
      let(:string) { 'mongodb://localhost:27017' }
      it "returns the original string" do
        expect(uri.to_s).to eq(string)
      end
    end
  end

  describe '#servers' do
    let(:string) { "#{scheme}#{servers}" }

    context 'single server' do
      let(:servers) { 'localhost' }

      it 'returns an array with the parsed server' do
        expect(uri.servers).to eq([servers])
      end

      include_examples "roundtrips string"
    end

    context 'single server with port' do
      let(:servers) { 'localhost:27017' }

      it 'returns an array with the parsed server' do
        expect(uri.servers).to eq([servers])
      end

      include_examples "roundtrips string"
    end

    context 'numerical ipv4 server' do
      let(:servers) { '127.0.0.1' }

      it 'returns an array with the parsed server' do
        expect(uri.servers).to eq([servers])
      end

      include_examples "roundtrips string"
    end

    context 'numerical ipv6 server' do
      let(:servers) { '[::1]:27107' }

      it 'returns an array with the parsed server' do
        expect(uri.servers).to eq([servers])
      end

      include_examples "roundtrips string"
    end

    context 'unix socket server' do
      let(:servers) { '%2Ftmp%2Fmongodb-27017.sock' }

      it 'returns an array with the parsed server' do
        expect(uri.servers).to eq([URI::DEFAULT_PARSER.unescape(servers)])
      end

      include_examples "roundtrips string"
    end

    context 'multiple servers' do
      let(:servers) { 'localhost,127.0.0.1' }

      it 'returns an array with the parsed servers' do
        expect(uri.servers).to eq(servers.split(','))
      end

      include_examples "roundtrips string"
    end

    context 'multiple servers with ports' do
      let(:servers) { '127.0.0.1:27107,localhost:27018' }

      it 'returns an array with the parsed servers' do
        expect(uri.servers).to eq(servers.split(','))
      end

      include_examples "roundtrips string"
    end
  end

  describe '#client_options' do

    let(:db)          { 'dummy_db' }
    let(:servers)     { 'localhost' }
    let(:string)      { "#{scheme}#{credentials}@#{servers}/#{db}" }
    let(:user)        { 'tyler' }
    let(:password)    { 's3kr4t' }
    let(:credentials) { "#{user}:#{password}" }

    let(:options) do
      uri.client_options
    end

    it 'includes the database in the options' do
      expect(options[:database]).to eq('dummy_db')
    end

    it 'includes the user in the options' do
      expect(options[:user]).to eq(user)
    end

    it 'includes the password in the options' do
      expect(options[:password]).to eq(password)
    end

    include_examples "roundtrips string"
  end

  describe '#credentials' do
    let(:servers)    { 'localhost' }
    let(:string)   { "#{scheme}#{credentials}@#{servers}" }
    let(:user)     { 'tyler' }

    context 'username provided' do
      let(:credentials) { "#{user}:" }

      it 'returns the username' do
        expect(uri.credentials[:user]).to eq(user)
      end

      it "roundtrips string without the colon" do
        expect(uri.to_s).to eq("mongodb://tyler@localhost")
      end
    end

    context 'username and password provided' do
      let(:password)    { 's3kr4t' }
      let(:credentials) { "#{user}:#{password}" }

      it 'returns the username' do
        expect(uri.credentials[:user]).to eq(user)
      end

      it 'returns the password' do
        expect(uri.credentials[:password]).to eq(password)
      end

      include_examples "roundtrips string"
    end
  end

  describe '#database' do
    let(:servers)  { 'localhost' }
    let(:string) { "#{scheme}#{servers}/#{db}" }
    let(:db)     { 'auth-db' }

    context 'database provided' do
      it 'returns the database name' do
        expect(uri.database).to eq(db)
      end

      include_examples "roundtrips string"
    end
  end

  describe '#uri_options' do
    let(:servers)  { 'localhost' }
    let(:string) { "#{scheme}#{servers}/?#{options}" }

    context 'when no options were provided' do
      let(:string) { "#{scheme}#{servers}" }

      it 'returns an empty hash' do
        expect(uri.uri_options).to be_empty
      end

      include_examples "roundtrips string"
    end

    context 'write concern options provided' do

      context 'numerical w value' do
        let(:options) { 'w=1' }
        let(:concern) { Mongo::Options::Redacted.new(:w => 1)}

        it 'sets the write concern options' do
          expect(uri.uri_options[:write_concern]).to eq(concern)
        end

        it 'sets the options on a client created with the uri' do
          client = new_local_client_nmio(string)
          expect(client.options[:write_concern]).to eq(concern)
        end

        include_examples "roundtrips string"
      end

      context 'w=majority' do
        let(:options) { 'w=majority' }
        let(:concern) { Mongo::Options::Redacted.new(:w => :majority) }

        it 'sets the write concern options' do
          expect(uri.uri_options[:write_concern]).to eq(concern)
        end

        it 'sets the options on a client created with the uri' do
          client = new_local_client_nmio(string)
          expect(client.options[:write_concern]).to eq(concern)
        end

        include_examples "roundtrips string"
      end

      context 'journal' do
        let(:options) { 'journal=true' }
        let(:concern) { Mongo::Options::Redacted.new(:j => true) }

        it 'sets the write concern options' do
          expect(uri.uri_options[:write_concern]).to eq(concern)
        end

        it 'sets the options on a client created with the uri' do
          client = new_local_client_nmio(string)
          expect(client.options[:write_concern]).to eq(concern)
        end

        include_examples "roundtrips string"
      end

      context 'fsync' do
        let(:options) { 'fsync=true' }
        let(:concern) { Mongo::Options::Redacted.new(:fsync => true) }

        it 'sets the write concern options' do
          expect(uri.uri_options[:write_concern]).to eq(concern)
        end

        it 'sets the options on a client created with the uri' do
          client = new_local_client_nmio(string)
          expect(client.options[:write_concern]).to eq(concern)
        end

        include_examples "roundtrips string"
      end

      context 'wtimeoutMS' do
        let(:timeout) { 1234 }
        let(:options) { "w=2&wtimeoutMS=#{timeout}" }
        let(:concern) { Mongo::Options::Redacted.new(:w => 2, :wtimeout => timeout) }

        it 'sets the write concern options' do
          expect(uri.uri_options[:write_concern]).to eq(concern)
        end

        it 'sets the options on a client created with the uri' do
          client = new_local_client_nmio(string)
          expect(client.options[:write_concern]).to eq(concern)
        end

        it "roundtrips the string with camelCase" do
          expect(uri.to_s).to eq("mongodb://localhost/?w=2&wTimeoutMS=1234")
        end
      end
    end

    context 'read preference option provided' do
      let(:options) { "readPreference=#{mode}" }

      context 'primary' do
        let(:mode) { 'primary' }
        let(:read) { Mongo::Options::Redacted.new(:mode => :primary) }

        it 'sets the read preference' do
          expect(uri.uri_options[:read]).to eq(read)
        end

        it 'sets the options on a client created with the uri' do
          client = new_local_client_nmio(string)
          expect(client.options[:read]).to eq(read)
        end

        include_examples "roundtrips string"
      end

      context 'primaryPreferred' do
        let(:mode) { 'primaryPreferred' }
        let(:read) { Mongo::Options::Redacted.new(:mode => :primary_preferred) }

        it 'sets the read preference' do
          expect(uri.uri_options[:read]).to eq(read)
        end

        it 'sets the options on a client created with the uri' do
          client = new_local_client_nmio(string)
          expect(client.options[:read]).to eq(read)
        end

        include_examples "roundtrips string"
      end

      context 'secondary' do
        let(:mode) { 'secondary' }
        let(:read) { Mongo::Options::Redacted.new(:mode => :secondary) }

        it 'sets the read preference' do
          expect(uri.uri_options[:read]).to eq(read)
        end

        it 'sets the options on a client created with the uri' do
          client = new_local_client_nmio(string)
          expect(client.options[:read]).to eq(read)
        end

        include_examples "roundtrips string"
      end

      context 'secondaryPreferred' do
        let(:mode) { 'secondaryPreferred' }
        let(:read) { Mongo::Options::Redacted.new(:mode => :secondary_preferred) }

        it 'sets the read preference' do
          expect(uri.uri_options[:read]).to eq(read)
        end

        it 'sets the options on a client created with the uri' do
          client = new_local_client_nmio(string)
          expect(client.options[:read]).to eq(read)
        end

        include_examples "roundtrips string"
      end

      context 'nearest' do
        let(:mode) { 'nearest' }
        let(:read) { Mongo::Options::Redacted.new(:mode => :nearest) }

        it 'sets the read preference' do
          expect(uri.uri_options[:read]).to eq(read)
        end

        it 'sets the options on a client created with the uri' do
          client = new_local_client_nmio(string)
          expect(client.options[:read]).to eq(read)
        end

        include_examples "roundtrips string"
      end
    end

    context 'read preference tags provided' do

      context 'single read preference tag set' do
        let(:options) do
          'readPreferenceTags=dc:ny,rack:1'
        end

        let(:read) do
          Mongo::Options::Redacted.new(:tag_sets => [{ 'dc' => 'ny', 'rack' => '1' }])
        end

        it 'sets the read preference tag set' do
          expect(uri.uri_options[:read]).to eq(read)
        end

        it 'sets the options on a client created with the uri' do
          client = new_local_client_nmio(string)
          expect(client.options[:read]).to eq(read)
        end

        include_examples "roundtrips string"
      end

      context 'multiple read preference tag sets' do
        let(:options) do
          'readPreferenceTags=dc:ny&readPreferenceTags=dc:bos'
        end

        let(:read) do
          Mongo::Options::Redacted.new(:tag_sets => [{ 'dc' => 'ny' }, { 'dc' => 'bos' }])
        end

        it 'sets the read preference tag sets' do
          expect(uri.uri_options[:read]).to eq(read)
        end

        it 'sets the options on a client created with the uri' do
          client = new_local_client_nmio(string)
          expect(client.options[:read]).to eq(read)
        end

        include_examples "roundtrips string"
      end
    end

    context 'read preference max staleness option provided' do

      let(:options) do
        'readPreference=Secondary&maxStalenessSeconds=120'
      end

      let(:read) do
        Mongo::Options::Redacted.new(mode: :secondary, :max_staleness => 120)
      end

      it 'sets the read preference max staleness in seconds' do
        expect(uri.uri_options[:read]).to eq(read)
      end

      it 'sets the options on a client created with the uri' do
          client = new_local_client_nmio(string)
        expect(client.options[:read]).to eq(read)
      end

      context 'when the read preference and max staleness combination is invalid' do

        context 'when max staleness is combined with read preference mode primary' do

          let(:options) do
            'readPreference=primary&maxStalenessSeconds=120'
          end

          it 'raises an exception when read preference is accessed on the client' do
            client = new_local_client_nmio(string)
            expect {
              client.server_selector
            }.to raise_exception(Mongo::Error::InvalidServerPreference)
          end
        end

        context 'when the max staleness value is too small' do

          let(:options) do
            'readPreference=secondary&maxStalenessSeconds=89'
          end

          it 'does not raise an exception and drops the option' do
            client = new_local_client_nmio(string)
            expect(client.read_preference).to eq(BSON::Document.new(mode: :secondary))
          end

          it "returns the string without the dropped option" do
            expect(uri.to_s).to eq("mongodb://localhost/?readPreference=secondary")
          end
        end
      end
    end

    context 'replica set option provided' do
      let(:rs_name) { 'dummy_rs' }
      let(:options) { "replicaSet=#{rs_name}" }

      it 'sets the replica set option' do
        expect(uri.uri_options[:replica_set]).to eq(rs_name)
      end

      it 'sets the options on a client created with the uri' do
        client = new_local_client_nmio(string)
        expect(client.options[:replica_set]).to eq(rs_name)
      end

      include_examples "roundtrips string"
    end

    context 'auth mechanism provided' do
      let(:string)      { "#{scheme}#{credentials}@#{servers}/?#{options}" }
      let(:user)        { 'tyler' }
      let(:password)    { 's3kr4t' }
      let(:credentials) { "#{user}:#{password}" }
      let(:options)     { "authMechanism=#{mechanism}" }

      context 'plain' do
        let(:mechanism) { 'PLAIN' }
        let(:expected) { :plain }

        it 'sets the auth mechanism to :plain' do
          expect(uri.uri_options[:auth_mech]).to eq(expected)
        end

        it 'sets the options on a client created with the uri' do
          client = new_local_client_nmio(string)
          expect(client.options[:auth_mech]).to eq(expected)
        end

        it 'is case-insensitive' do
          client = new_local_client_nmio(string.downcase)
          expect(client.options[:auth_mech]).to eq(expected)
        end

        include_examples "roundtrips string"

        context 'when mechanism_properties are provided' do
          let(:options) { "authMechanism=#{mechanism}&authMechanismProperties=CANONICALIZE_HOST_NAME:true" }

          it 'does not allow a client to be created' do
            expect {
              new_local_client_nmio(string)
            }.to raise_error(Mongo::Auth::InvalidConfiguration, /mechanism_properties are not supported/)
          end
        end
      end

      context 'mongodb-cr' do
        let(:mechanism) { 'MONGODB-CR' }
        let(:expected) { :mongodb_cr }

        it 'sets the auth mechanism to :mongodb_cr' do
          expect(uri.uri_options[:auth_mech]).to eq(expected)
        end

        it 'sets the options on a client created with the uri' do
          client = new_local_client_nmio(string)
          expect(client.options[:auth_mech]).to eq(expected)
        end

        it 'is case-insensitive' do
          client = new_local_client_nmio(string.downcase)
          expect(client.options[:auth_mech]).to eq(expected)
        end

        include_examples "roundtrips string"

        context 'when mechanism_properties are provided' do
          let(:options) { "authMechanism=#{mechanism}&authMechanismProperties=CANONICALIZE_HOST_NAME:true" }

          it 'does not allow a client to be created' do
            expect {
              new_local_client_nmio(string)
            }.to raise_error(Mongo::Auth::InvalidConfiguration, /mechanism_properties are not supported/)
          end
        end
      end

      context 'gssapi' do
        require_mongo_kerberos

        let(:mechanism) { 'GSSAPI' }
        let(:expected) { :gssapi }
        let(:client) { new_local_client_nmio(string) }

        it 'sets the auth mechanism to :gssapi' do
          expect(uri.uri_options[:auth_mech]).to eq(expected)
        end

        it 'sets the options on a client created with the uri' do
          expect(client.options[:auth_mech]).to eq(expected)
        end

        it 'is case-insensitive' do
          client = new_local_client_nmio(string.downcase)
          expect(client.options[:auth_mech]).to eq(expected)
        end

        include_examples "roundtrips string"

        context 'when auth source is invalid' do
          let(:options) { "authMechanism=#{mechanism}&authSource=foo" }

          it 'does not allow a client to be created' do
            expect {
              client
            }.to raise_error(Mongo::Auth::InvalidConfiguration, /invalid auth source/)
          end
        end

        context 'when mechanism_properties are provided' do
          let(:options) { "authMechanism=#{mechanism}&authMechanismProperties=SERVICE_NAME:other,CANONICALIZE_HOST_NAME:true" }

          it 'sets the options on a client created with the uri' do
            expect(client.options[:auth_mech_properties]).to eq({ 'canonicalize_host_name' => true, 'service_name' => 'other' })
          end

          include_examples "roundtrips string"

          context 'when a mapping value is missing' do
            let(:options) { "authMechanism=#{mechanism}&authMechanismProperties=SERVICE_NAME:,CANONICALIZE_HOST_NAME:" }

            it 'sets the options to defaults' do
              expect(client.options[:auth_mech_properties]).to eq({ 'service_name' => 'mongodb' })
            end

            it "roundtrips the string" do
              expect(uri.to_s).to eq("mongodb://tyler:s3kr4t@localhost/?authMechanism=GSSAPI")
            end
          end

          context 'when a mapping value is missing but another is present' do
            let(:options) { "authMechanism=#{mechanism}&authMechanismProperties=SERVICE_NAME:foo,CANONICALIZE_HOST_NAME:" }

            it 'only sets the present value' do
              expect(client.options[:auth_mech_properties]).to eq({ 'service_name' => 'foo' })
            end

            it "roundtrips the string" do
              expect(uri.to_s).to eq("mongodb://tyler:s3kr4t@localhost/?authMechanism=GSSAPI&authMechanismProperties=SERVICE_NAME:foo")
            end
          end
        end
      end

      context 'scram-sha-1' do
        let(:mechanism) { 'SCRAM-SHA-1' }
        let(:expected) { :scram }

        it 'sets the auth mechanism to :scram' do
          expect(uri.uri_options[:auth_mech]).to eq(expected)
        end

        it 'sets the options on a client created with the uri' do
          client = new_local_client_nmio(string)
          expect(client.options[:auth_mech]).to eq(expected)
        end

        it 'is case-insensitive' do
          client = new_local_client_nmio(string.downcase)
          expect(client.options[:auth_mech]).to eq(expected)
        end

        include_examples "roundtrips string"

        context 'when mechanism_properties are provided' do
          let(:options) { "authMechanism=#{mechanism}&authMechanismProperties=CANONICALIZE_HOST_NAME:true" }

          it 'does not allow a client to be created' do
            expect {
              new_local_client_nmio(string)
            }.to raise_error(Mongo::Auth::InvalidConfiguration, /mechanism_properties are not supported/)
          end
        end
      end

      context 'mongodb-x509' do
        let(:mechanism) { 'MONGODB-X509' }
        let(:expected)  { :mongodb_x509 }
        let(:credentials)  { user }

        it 'sets the auth mechanism to :mongodb_x509' do
          expect(uri.uri_options[:auth_mech]).to eq(expected)
        end

        it 'sets the options on a client created with the uri' do
          client = new_local_client_nmio(string)
          expect(client.options[:auth_mech]).to eq(expected)
        end

        it 'is case-insensitive' do
          client = new_local_client_nmio(string.downcase)
          expect(client.options[:auth_mech]).to eq(expected)
        end

        include_examples "roundtrips string"

        context 'when auth source is invalid' do
          let(:options) { "authMechanism=#{mechanism}&authSource=foo" }

          it 'does not allow a client to be created' do
            expect {
              new_local_client_nmio(string)
            }.to raise_error(Mongo::Auth::InvalidConfiguration, /invalid auth source/)
          end
        end

        context 'when a username is not provided' do
          let(:string) { "#{scheme}#{servers}/?#{options}" }

          it 'recognizes the mechanism with no username' do
            client = new_local_client_nmio(string.downcase)
            expect(client.options[:auth_mech]).to eq(expected)
            expect(client.options[:user]).to be_nil
          end

          include_examples "roundtrips string"
        end

        context 'when a password is provided' do
          let(:credentials) { "#{user}:#{password}"}
          let(:password) { 's3kr4t' }

          it 'does not allow a client to be created' do
            expect do
              new_local_client_nmio(string)
            end.to raise_error(Mongo::Auth::InvalidConfiguration, /Password is not supported/)
          end
        end

        context 'when mechanism_properties are provided' do
          let(:options) { "authMechanism=#{mechanism}&authMechanismProperties=CANONICALIZE_HOST_NAME:true" }

          it 'does not allow a client to be created' do
            expect {
              new_local_client_nmio(string)
            }.to raise_error(Mongo::Auth::InvalidConfiguration, /mechanism_properties are not supported/)
          end
        end
      end
    end

    context 'auth mechanism is not provided' do
      let(:string) { "#{scheme}#{credentials}@#{servers}/" }

      context 'with no credentials' do
        let(:string) { "#{scheme}#{servers}" }

        it 'sets user and password as nil' do
          expect(uri.credentials[:user]).to be_nil
          expect(uri.credentials[:password]).to be_nil
        end

        it 'sets the options on a client created with the uri' do
          client = new_local_client_nmio(string)
          expect(client.options[:user]).to be_nil
          expect(client.options[:password]).to be_nil
        end

        include_examples "roundtrips string"
      end

      context 'with empty credentials' do
        let(:credentials) { '' }

        it 'sets user as an empty string and password as nil' do
          expect(uri.credentials[:user]).to eq('')
          expect(uri.credentials[:password]).to be_nil
        end

        it 'does not allow a client to be created with default auth mechanism' do
          expect do
            new_local_client_nmio(string)
          end.to raise_error(Mongo::Auth::InvalidConfiguration, /Empty username is not supported/)
        end
      end
    end

    context 'auth source provided' do
      let(:options) { "authSource=#{source}" }

      context 'regular db' do
        let(:source) { 'foo' }

        it 'sets the auth source to the database' do
          expect(uri.uri_options[:auth_source]).to eq(source)
        end

        it 'sets the options on a client created with the uri' do
          client = new_local_client_nmio(string)
          expect(client.options[:auth_source]).to eq(source)
        end

        include_examples "roundtrips string"
      end
    end

    context 'auth mechanism properties provided' do

      shared_examples 'sets options in the expected manner' do
        it 'preserves case in auth mechanism properties returned from URI' do
          expect(uri.uri_options[:auth_mech_properties]).to eq(expected_uri_options)
        end

        it 'downcases auth mechanism properties keys in client options' do
          client = new_local_client_nmio(string)
          expect(client.options[:auth_mech_properties]).to eq(expected_client_options)
        end
      end

      context 'service_name' do
        let(:options) do
          "authMechanismProperties=SERVICE_name:#{service_name}"
        end

        let(:service_name) { 'foo' }

        let(:expected_uri_options) do
          Mongo::Options::Redacted.new(
            SERVICE_name: service_name,
          )
        end

        let(:expected_client_options) do
          Mongo::Options::Redacted.new(
            service_name: service_name,
          )
        end

        include_examples 'sets options in the expected manner'
        include_examples "roundtrips string"
      end

      context 'canonicalize_host_name' do
        let(:options) do
          "authMechanismProperties=CANONICALIZE_HOST_name:#{canonicalize_host_name}"
        end

        let(:canonicalize_host_name) { 'true' }

        let(:expected_uri_options) do
          Mongo::Options::Redacted.new(
            CANONICALIZE_HOST_name: true,
          )
        end

        let(:expected_client_options) do
          Mongo::Options::Redacted.new(
            canonicalize_host_name: true,
          )
        end

        include_examples 'sets options in the expected manner'
        include_examples "roundtrips string"
      end

      context 'service_realm' do
        let(:options) do
          "authMechanismProperties=SERVICE_realm:#{service_realm}"
        end

        let(:service_realm) { 'dumdum' }

        let(:expected_uri_options) do
          Mongo::Options::Redacted.new(
            SERVICE_realm: service_realm,
          )
        end

        let(:expected_client_options) do
          Mongo::Options::Redacted.new(
            service_realm: service_realm,
          )
        end

        include_examples 'sets options in the expected manner'
        include_examples "roundtrips string"
      end

      context 'multiple properties' do
        let(:options) do
          "authMechanismProperties=SERVICE_realm:#{service_realm}," +
            "CANONICALIZE_HOST_name:#{canonicalize_host_name}," +
            "SERVICE_name:#{service_name}"
        end

        let(:service_name) { 'foo' }
        let(:canonicalize_host_name) { 'true' }
        let(:service_realm) { 'dumdum' }

        let(:expected_uri_options) do
          Mongo::Options::Redacted.new(
            SERVICE_name: service_name,
            CANONICALIZE_HOST_name: true,
            SERVICE_realm: service_realm,
          )
        end

        let(:expected_client_options) do
          Mongo::Options::Redacted.new(
            service_name: service_name,
            canonicalize_host_name: true,
            service_realm: service_realm,
          )
        end

        include_examples 'sets options in the expected manner'
        include_examples "roundtrips string"
      end
    end

    context 'connectTimeoutMS' do
      let(:options) { "connectTimeoutMS=4567" }

      it 'sets the the connect timeout' do
        expect(uri.uri_options[:connect_timeout]).to eq(4.567)
      end

      include_examples "roundtrips string"
    end

    context 'socketTimeoutMS' do
      let(:options) { "socketTimeoutMS=8910" }

      it 'sets the socket timeout' do
        expect(uri.uri_options[:socket_timeout]).to eq(8.910)
      end

      include_examples "roundtrips string"
    end

    context 'when providing serverSelectionTimeoutMS' do

      let(:options) { "serverSelectionTimeoutMS=3561" }

      it 'sets the the connect timeout' do
        expect(uri.uri_options[:server_selection_timeout]).to eq(3.561)
      end

      include_examples "roundtrips string"
    end

    context 'when providing localThresholdMS' do

      let(:options) { "localThresholdMS=3561" }

      it 'sets the the connect timeout' do
        expect(uri.uri_options[:local_threshold]).to eq(3.561)
      end

      include_examples "roundtrips string"
    end

    context 'when providing maxPoolSize' do

      let(:max_pool_size) { 10 }
      let(:options) { "maxPoolSize=#{max_pool_size}" }

      it 'sets the max pool size option' do
        expect(uri.uri_options[:max_pool_size]).to eq(max_pool_size)
      end

      include_examples "roundtrips string"
    end

    context 'when providing minPoolSize' do

      let(:min_pool_size) { 5 }
      let(:options) { "minPoolSize=#{min_pool_size}" }

      it 'sets the min pool size option' do
        expect(uri.uri_options[:min_pool_size]).to eq(min_pool_size)
      end

      include_examples "roundtrips string"
    end

    context 'when providing srvMaxHosts with non-SRV URI' do

      let(:srv_max_hosts) { 5 }
      let(:options) { "srvMaxHosts=#{srv_max_hosts}" }

      it 'raises an error' do
        lambda do
          uri
        end.should raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'when providing srvServiceName with non-SRV URI' do

      let(:scheme) { "mongodb+srv://" }
      let(:srv_service_name) { "customname" }
      let(:options) { "srvServiceName=#{srv_service_name}" }

      it 'raises an error' do
        lambda do
          uri
        end.should raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'when providing waitQueueTimeoutMS' do

      let(:wait_queue_timeout) { 500 }
      let(:options) { "waitQueueTimeoutMS=#{wait_queue_timeout}" }

      it 'sets the wait queue timeout option' do
        expect(uri.uri_options[:wait_queue_timeout]).to eq(0.5)
      end

      include_examples "roundtrips string"
    end

    context 'ssl' do
      let(:options) { "ssl=#{ssl}" }

      context 'true' do
        let(:ssl) { true }

        it 'sets the ssl option to true' do
          expect(uri.uri_options[:ssl]).to be true
        end

        it "returns the ssl as tls from to_s" do
          expect(uri.to_s).to eq("mongodb://localhost/?tls=true")
        end
      end

      context 'false' do
        let(:ssl) { false }

        it 'sets the ssl option to false' do
          expect(uri.uri_options[:ssl]).to be false
        end

        it "returns the ssl as tls from to_s" do
          expect(uri.to_s).to eq("mongodb://localhost/?tls=false")
        end
      end
    end

    context 'grouped and non-grouped options provided' do
      let(:options) { 'w=1&ssl=true' }

      it 'do not overshadow top level options' do
        expect(uri.uri_options).not_to be_empty
      end

      it "returns the ssl as tls from to_s" do
        expect(uri.to_s).to eq("mongodb://localhost/?w=1&tls=true")
      end
    end

    context 'when an invalid option is provided' do

      let(:options) { 'invalidOption=10' }

      let(:uri_options) do
        uri.uri_options
      end

      it 'does not raise an exception' do
        expect(uri_options).to be_empty
      end

      context 'when an invalid option is combined with valid options' do

        let(:options) { 'invalidOption=10&waitQueueTimeoutMS=500&ssl=true' }

        it 'does not raise an exception' do
          expect(uri_options).not_to be_empty
        end

        it 'sets the valid options' do
          expect(uri_options[:wait_queue_timeout]).to eq(0.5)
          expect(uri_options[:ssl]).to be true
        end
      end
    end

    context 'when an app name option is provided' do
      let(:options) { "appname=uri_test" }

      it 'sets the app name on the client' do
        client = new_local_client_nmio(string)
        expect(client.options[:app_name]).to eq('uri_test')
      end

      it "roundtrips the string with camelCase" do
        expect(uri.to_s).to eq("mongodb://localhost/?appName=uri_test")
      end
    end

    context 'when a supported compressors option is provided' do
      let(:options) { "compressors=zlib" }

      it 'sets the compressors as an array on the client' do
        client = new_local_client_nmio(string)
        expect(client.options[:compressors]).to eq(['zlib'])
      end

      include_examples "roundtrips string"
    end

    context 'when a non-supported compressors option is provided' do
      let(:options) { "compressors=snoopy" }

      let(:client) do
        client = new_local_client_nmio(string)
      end

      it 'sets no compressors on the client and warns' do
        expect(Mongo::Logger.logger).to receive(:warn)
        expect(client.options[:compressors]).to be_nil
      end

      include_examples "roundtrips string"
    end

    context 'when a zlibCompressionLevel option is provided' do
      let(:options) { "zlibCompressionLevel=6" }

      it 'sets the zlib compression level on the client' do
        client = new_local_client_nmio(string)
        expect(client.options[:zlib_compression_level]).to eq(6)
      end

      include_examples "roundtrips string"
    end
  end
end
