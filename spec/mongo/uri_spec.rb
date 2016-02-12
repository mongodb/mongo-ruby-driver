require 'spec_helper'

describe Mongo::URI do
  let(:scheme) { 'mongodb://' }
  let(:uri) { described_class.new(string) }

describe 'invalid uris' do

    context 'string is not uri' do

      let(:string) { 'tyler' }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'empty string' do

      let(:string) { '' }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
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

    context 'mongodb://example.com?w=1' do

      let(:string) { 'mongodb://example.com?w=1' }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb://example.com/?w' do

      let(:string) { 'mongodb://example.com/?w' }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
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

  describe '#servers' do
    let(:string) { "#{scheme}#{servers}" }

    context 'single server' do
      let(:servers) { 'localhost' }

      it 'returns an array with the parsed server' do
        expect(uri.servers).to eq([servers])
      end
    end

    context 'single server with port' do
      let(:servers) { 'localhost:27017' }

      it 'returns an array with the parsed server' do
        expect(uri.servers).to eq([servers])
      end
    end

    context 'numerical ipv4 server' do
      let(:servers) { '127.0.0.1' }

      it 'returns an array with the parsed server' do
        expect(uri.servers).to eq([servers])
      end
    end

    context 'numerical ipv6 server' do
      let(:servers) { '[::1]:27107' }

      it 'returns an array with the parsed server' do
        expect(uri.servers).to eq([servers])
      end
    end

    context 'unix socket server' do
      let(:servers) { '%2Ftmp%2Fmongodb-27017.sock' }

      it 'returns an array with the parsed server' do
        expect(uri.servers).to eq([servers])
      end
    end

    context 'multiple servers' do
      let(:servers) { 'localhost,127.0.0.1' }

      it 'returns an array with the parsed servers' do
        expect(uri.servers).to eq(servers.split(','))
      end
    end

    context 'multiple servers with ports' do
      let(:servers) { '127.0.0.1:27107,localhost:27018' }

      it 'returns an array with the parsed servers' do
        expect(uri.servers).to eq(servers.split(','))
      end
    end
  end

  describe '#client_options' do

    let(:db)          { TEST_DB }
    let(:servers)     { 'localhost' }
    let(:string)      { "#{scheme}#{credentials}@#{servers}/#{db}" }
    let(:user)        { 'tyler' }
    let(:password)    { 's3kr4t' }
    let(:credentials) { "#{user}:#{password}" }

    let(:options) do
      uri.client_options
    end

    it 'includes the database in the options' do
      expect(options[:database]).to eq(TEST_DB)
    end

    it 'includes the credentials in the options' do
      expect(options[:user]).to eq(user)
    end

    it 'includes the options in the options' do
      expect(options[:password]).to eq(password)
    end
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
    end

    context 'write concern options provided' do

      context 'numerical w value' do
        let(:options) { 'w=1' }
        let(:concern) { Mongo::Options::Redacted.new(:w => 1)}

        it 'sets the write concern options' do
          expect(uri.uri_options[:write]).to eq(concern)
        end
      end

      context 'w=majority' do
        let(:options) { 'w=majority' }
        let(:concern) { Mongo::Options::Redacted.new(:w => :majority) }

        it 'sets the write concern options' do
          expect(uri.uri_options[:write]).to eq(concern)
        end
      end

      context 'journal' do
        let(:options) { 'journal=true' }
        let(:concern) { Mongo::Options::Redacted.new(:j => true) }

        it 'sets the write concern options' do
          expect(uri.uri_options[:write]).to eq(concern)
        end
      end

      context 'fsync' do
        let(:options) { 'fsync=true' }
        let(:concern) { Mongo::Options::Redacted.new(:fsync => true) }

        it 'sets the write concern options' do
          expect(uri.uri_options[:write]).to eq(concern)
        end
      end

      context 'wtimeoutMS' do
        let(:timeout) { 1234 }
        let(:options) { "w=2&wtimeoutMS=#{timeout}" }
        let(:concern) { Mongo::Options::Redacted.new(:w => 2, :timeout => timeout) }

        it 'sets the write concern options' do
          expect(uri.uri_options[:write]).to eq(concern)
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
      end

      context 'primaryPreferred' do
        let(:mode) { 'primaryPreferred' }
        let(:read) { Mongo::Options::Redacted.new(:mode => :primary_preferred) }

        it 'sets the read preference' do
          expect(uri.uri_options[:read]).to eq(read)
        end
      end

      context 'secondary' do
        let(:mode) { 'secondary' }
        let(:read) { Mongo::Options::Redacted.new(:mode => :secondary) }

        it 'sets the read preference' do
          expect(uri.uri_options[:read]).to eq(read)
        end
      end

      context 'secondaryPreferred' do
        let(:mode) { 'secondaryPreferred' }
        let(:read) { Mongo::Options::Redacted.new(:mode => :secondary_preferred) }

        it 'sets the read preference' do
          expect(uri.uri_options[:read]).to eq(read)
        end
      end

      context 'nearest' do
        let(:mode) { 'nearest' }
        let(:read) { Mongo::Options::Redacted.new(:mode => :nearest) }

        it 'sets the read preference' do
          expect(uri.uri_options[:read]).to eq(read)
        end
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
      end
    end

    context 'replica set option provided' do
      let(:rs_name) { TEST_SET }
      let(:options) { "replicaSet=#{rs_name}" }

      it 'sets the replica set option' do
        expect(uri.uri_options[:replica_set]).to eq(rs_name)
      end
    end

    context 'auth mechanism provided' do
      let(:options) { "authMechanism=#{mechanism}" }

      context 'plain' do
        let(:mechanism) { 'PLAIN' }
        let(:expected) { :plain }

        it 'sets the auth mechanism to :plain' do
          expect(uri.uri_options[:auth_mech]).to eq(expected)
        end
      end

      context 'mongodb-cr' do
        let(:mechanism) { 'MONGODB-CR' }
        let(:expected) { :mongodb_cr }

        it 'sets the auth mechanism to :mongodb_cr' do
          expect(uri.uri_options[:auth_mech]).to eq(expected)
        end
      end

      context 'gssapi' do
        let(:mechanism) { 'GSSAPI' }
        let(:expected) { :gssapi }

        it 'sets the auth mechanism to :gssapi' do
          expect(uri.uri_options[:auth_mech]).to eq(expected)
        end
      end

      context 'scram-sha-1' do
        let(:mechanism) { 'SCRAM-SHA-1' }
        let(:expected) { :scram }

        it 'sets the auth mechanism to :scram' do
          expect(uri.uri_options[:auth_mech]).to eq(expected)
        end
      end
    end

    context 'auth source provided' do
      let(:options) { "authSource=#{source}" }

      context 'regular db' do
        let(:source) { 'foo' }
        let(:auth) { Mongo::Options::Redacted.new(:source => 'foo') }

        it 'sets the auth source to the database' do
          expect(uri.uri_options[:auth]).to eq(auth)
        end
      end

      context '$external' do
        let(:source) { '$external' }
        let(:auth) { Mongo::Options::Redacted.new(:source => :external) }

        it 'sets the auth source to :external' do
          expect(uri.uri_options[:auth]).to eq(auth)
        end
      end
    end

    context 'auth mechanism properties provided' do

      context 'service_name' do
        let(:options) do
          "authMechanismProperties=SERVICE_NAME:#{service_name}"
        end

        let(:service_name) { 'foo' }
        let(:auth) do
          Mongo::Options::Redacted.new(auth_mech_properties: { service_name: service_name })
        end

        it 'sets the auth mechanism properties' do
          expect(uri.uri_options[:auth]).to eq(auth)
        end
      end

      context 'canonicalize_host_name' do
        let(:options) do
          "authMechanismProperties=CANONICALIZE_HOST_NAME:#{canonicalize_host_name}"
        end

        let(:canonicalize_host_name) { 'true' }
        let(:auth) do
          Mongo::Options::Redacted.new(auth_mech_properties: { canonicalize_host_name: true })
        end

        it 'sets the auth mechanism properties' do
          expect(uri.uri_options[:auth]).to eq(auth)
        end
      end

      context 'service_realm' do
        let(:options) do
          "authMechanismProperties=SERVICE_REALM:#{service_realm}"
        end

        let(:service_realm) { 'dumdum' }
        let(:auth) do
          Mongo::Options::Redacted.new(auth_mech_properties: { service_realm: service_realm })
        end

        it 'sets the auth mechanism properties' do
          expect(uri.uri_options[:auth]).to eq(auth)
        end
      end

      context 'multiple properties' do
        let(:options) do
          "authMechanismProperties=SERVICE_REALM:#{service_realm}," +
            "CANONICALIZE_HOST_NAME:#{canonicalize_host_name}," +
            "SERVICE_NAME:#{service_name}"
        end

        let(:service_name) { 'foo' }
        let(:canonicalize_host_name) { 'true' }
        let(:service_realm) { 'dumdum' }

        let(:auth) do
          Mongo::Options::Redacted.new(auth_mech_properties: { service_name: service_name,
                                                               canonicalize_host_name: true,
                                                               service_realm: service_realm })
        end

        it 'sets the auth mechanism properties' do
          expect(uri.uri_options[:auth]).to eq(auth)
        end
      end
    end

    context 'connectTimeoutMS' do
      let(:options) { "connectTimeoutMS=4567" }

      it 'sets the the connect timeout' do
        expect(uri.uri_options[:connect_timeout]).to eq(4.567)
      end
    end

    context 'socketTimeoutMS' do
      let(:options) { "socketTimeoutMS=8910" }

      it 'sets the socket timeout' do
        expect(uri.uri_options[:socket_timeout]).to eq(8.910)
      end
    end

    context 'when providing serverSelectionTimeoutMS' do

      let(:options) { "serverSelectionTimeoutMS=3561" }

      it 'sets the the connect timeout' do
        expect(uri.uri_options[:server_selection_timeout]).to eq(3.561)
      end
    end

    context 'when providing localThresholdMS' do

      let(:options) { "localThresholdMS=3561" }

      it 'sets the the connect timeout' do
        expect(uri.uri_options[:local_threshold]).to eq(3.561)
      end
    end

    context 'when providing maxPoolSize' do

      let(:max_pool_size) { 10 }
      let(:options) { "maxPoolSize=#{max_pool_size}" }

      it 'sets the max pool size option' do
        expect(uri.uri_options[:max_pool_size]).to eq(max_pool_size)
      end
    end

    context 'when providing minPoolSize' do

      let(:min_pool_size) { 5 }
      let(:options) { "minPoolSize=#{min_pool_size}" }

      it 'sets the min pool size option' do
        expect(uri.uri_options[:min_pool_size]).to eq(min_pool_size)
      end
    end

    context 'when providing waitQueueTimeoutMS' do

      let(:wait_queue_timeout) { 500 }
      let(:options) { "waitQueueTimeoutMS=#{wait_queue_timeout}" }

      it 'sets the wait queue timeout option' do
        expect(uri.uri_options[:wait_queue_timeout]).to eq(0.5)
      end
    end

    context 'ssl' do
      let(:options) { "ssl=#{ssl}" }

      context 'true' do
        let(:ssl) { true }

        it 'sets the ssl option to true' do
          expect(uri.uri_options[:ssl]).to be true
        end
      end

      context 'false' do
        let(:ssl) { false }

        it 'sets the ssl option to false' do
          expect(uri.uri_options[:ssl]).to be false
        end
      end
    end

    context 'grouped and non-grouped options provided' do
      let(:options) { 'w=1&ssl=true' }

      it 'do not overshadow top level options' do
        expect(uri.uri_options).not_to be_empty
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
  end
end
