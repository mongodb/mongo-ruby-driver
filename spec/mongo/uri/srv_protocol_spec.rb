require 'spec_helper'

describe Mongo::URI::SRVProtocol do

  let(:scheme) { 'mongodb+srv://' }
  let(:uri) { described_class.new(string) }

  before(:all) do
    # Since these tests assert on warnings being produced,
    # close clients to ensure background threads do not interfere with
    # their warnings.
    ClientRegistry.instance.close_all_clients
  end

  let(:client) do
    new_local_client(string, monitoring_io: false)
  end

  describe 'invalid uris' do

    context 'when there is more than one hostname' do

      let(:string) { "#{scheme}#{hosts}" }
      let(:hosts) { 'test5.test.build.10gen.cc,test6.test.build.10gen.cc' }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'when the the hostname has a port' do

      let(:string) { "#{scheme}#{hosts}" }
      let(:hosts) { 'test5.test.build.10gen.cc:8123' }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'when the host in URI does not have {hostname}, {domainname} and {tld}' do

      let(:string) { "#{scheme}#{hosts}" }
      let(:hosts) { '10gen.cc/' }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

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

    context 'mongodb+srv://' do

      let(:string) { "#{scheme}" }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb+srv://localhost::27017/' do

      let(:string) { "#{scheme}localhost::27017/" }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb+srv://::' do

      let(:string) { "#{scheme}::" }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb+srv://localhost,localhost::' do

      let(:string) { "#{scheme}localhost,localhost::" }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb+srv://localhost::27017,abc' do

      let(:string) { "#{scheme}localhost::27017,abc" }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb+srv://localhost:-1' do

      let(:string) { "#{scheme}localhost:-1" }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb+srv://localhost:0/' do

      let(:string) { "#{scheme}localhost:0/" }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb+srv://localhost:65536' do

      let(:string) { "#{scheme}localhost:65536" }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb+srv://localhost:foo' do

      let(:string) { "#{scheme}localhost:foo" }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb+srv://mongodb://[::1]:-1' do

      let(:string) { "#{scheme}mongodb://[::1]:-1" }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb+srv://[::1]:0/' do

      let(:string) { "#{scheme}[::1]:0/" }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb+srv://[::1]:65536' do

      let(:string) { "#{scheme}[::1]:65536" }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb+srv://[::1]:65536/' do

      let(:string) { "#{scheme}[::1]:65536/" }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb+srv://[::1]:foo' do

      let(:string) { "#{scheme}[::1]:foo" }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb+srv://example.com?w=1' do

      let(:string) { "#{scheme}example.com?w=1" }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'mongodb+srv://example.com/?w' do

      let(:string) { "#{scheme}example.com/?w" }

      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::Error::InvalidURI)
      end
    end
  end

  describe 'valid uris' do
    require_external_connectivity

    describe 'invalid query results' do

      context 'when there are too many TXT records' do

        let(:string) { "#{scheme}test6.test.build.10gen.cc/" }

        it 'raises an error' do
          expect { uri }.to raise_exception(Mongo::Error::InvalidTXTRecord)
        end
      end

      context 'when the TXT has an invalid option' do

        let(:string) { "#{scheme}test10.test.build.10gen.cc" }

        it 'raises an error' do
          expect { uri }.to raise_exception(Mongo::Error::InvalidTXTRecord)
        end
      end

      context 'when the SRV records domain does not match hostname used for the query' do

        let(:string) { "#{scheme}test12.test.build.10gen.cc" }

        it 'raises an error' do
          expect { uri }.to raise_exception(Mongo::Error::MismatchedDomain)
        end
      end

      context 'when the query returns no SRV records' do

        let(:string) { "#{scheme}test4.test.build.10gen.cc" }

        it 'raises an error' do
          expect { uri }.to raise_exception(Mongo::Error::NoSRVRecords)
        end
      end
    end

    describe '#servers' do
      let(:string) { "#{scheme}#{servers}" }

      context 'single server' do
        let(:servers) { 'test5.test.build.10gen.cc' }

        it 'returns an array with the parsed server' do
          expect(uri.servers).to eq(['localhost.test.build.10gen.cc:27017'])
        end
      end
    end

    describe '#client_options' do

      let(:db)          { SpecConfig.instance.test_db }
      let(:servers)     { 'test5.test.build.10gen.cc' }
      let(:string)      { "#{scheme}#{credentials}@#{servers}/#{db}" }
      let(:user)        { 'tyler' }
      let(:password)    { 's3kr4t' }
      let(:credentials) { "#{user}:#{password}" }

      let(:options) do
        uri.client_options
      end

      it 'includes the database in the options' do
        expect(options[:database]).to eq(SpecConfig.instance.test_db)
      end

      it 'includes the user in the options' do
        expect(options[:user]).to eq(user)
      end

      it 'includes the password in the options' do
        expect(options[:password]).to eq(password)
      end

      it 'sets ssl to true' do
        expect(options[:ssl]).to eq(true)
      end
    end

    describe '#credentials' do
      let(:servers)    { 'test5.test.build.10gen.cc' }
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
      let(:servers)  { 'test5.test.build.10gen.cc' }
      let(:string) { "#{scheme}#{servers}/#{db}" }
      let(:db)     { 'auth-db' }

      context 'database provided' do
        it 'returns the database name' do
          expect(uri.database).to eq(db)
        end
      end
    end

    describe '#uri_options' do
      let(:servers)  { 'test5.test.build.10gen.cc' }
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

          it 'sets the options on a client created with the uri' do
            expect(client.options[:write]).to eq(concern)
          end
        end

        context 'w=majority' do
          let(:options) { 'w=majority' }
          let(:concern) { Mongo::Options::Redacted.new(:w => :majority) }

          it 'sets the write concern options' do
            expect(uri.uri_options[:write]).to eq(concern)
          end

          it 'sets the options on a client created with the uri' do
            expect(client.options[:write]).to eq(concern)
          end
        end

        context 'journal' do
          let(:options) { 'journal=true' }
          let(:concern) { Mongo::Options::Redacted.new(:j => true) }

          it 'sets the write concern options' do
            expect(uri.uri_options[:write]).to eq(concern)
          end

          it 'sets the options on a client created with the uri' do
            expect(client.options[:write]).to eq(concern)
          end
        end

        context 'fsync' do
          let(:options) { 'fsync=true' }
          let(:concern) { Mongo::Options::Redacted.new(:fsync => true) }

          it 'sets the write concern options' do
            expect(uri.uri_options[:write]).to eq(concern)
          end

          it 'sets the options on a client created with the uri' do
            expect(client.options[:write]).to eq(concern)
          end
        end

        context 'wtimeoutMS' do
          let(:timeout) { 1234 }
          let(:options) { "w=2&wtimeoutMS=#{timeout}" }
          let(:concern) { Mongo::Options::Redacted.new(:w => 2, :timeout => timeout) }

          it 'sets the write concern options' do
            expect(uri.uri_options[:write]).to eq(concern)
          end

          it 'sets the options on a client created with the uri' do
            expect(client.options[:write]).to eq(concern)
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
            expect(client.options[:read]).to eq(read)
          end
        end

        context 'primaryPreferred' do
          let(:mode) { 'primaryPreferred' }
          let(:read) { Mongo::Options::Redacted.new(:mode => :primary_preferred) }

          it 'sets the read preference' do
            expect(uri.uri_options[:read]).to eq(read)
          end

          it 'sets the options on a client created with the uri' do
            expect(client.options[:read]).to eq(read)
          end
        end

        context 'secondary' do
          let(:mode) { 'secondary' }
          let(:read) { Mongo::Options::Redacted.new(:mode => :secondary) }

          it 'sets the read preference' do
            expect(uri.uri_options[:read]).to eq(read)
          end

          it 'sets the options on a client created with the uri' do
            expect(client.options[:read]).to eq(read)
          end
        end

        context 'secondaryPreferred' do
          let(:mode) { 'secondaryPreferred' }
          let(:read) { Mongo::Options::Redacted.new(:mode => :secondary_preferred) }

          it 'sets the read preference' do
            expect(uri.uri_options[:read]).to eq(read)
          end

          it 'sets the options on a client created with the uri' do
            expect(client.options[:read]).to eq(read)
          end
        end

        context 'nearest' do
          let(:mode) { 'nearest' }
          let(:read) { Mongo::Options::Redacted.new(:mode => :nearest) }

          it 'sets the read preference' do
            expect(uri.uri_options[:read]).to eq(read)
          end

          it 'sets the options on a client created with the uri' do
            expect(client.options[:read]).to eq(read)
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

          it 'sets the options on a client created with the uri' do
            expect(client.options[:read]).to eq(read)
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

          it 'sets the options on a client created with the uri' do
            expect(client.options[:read]).to eq(read)
          end
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
          expect(client.options[:read]).to eq(read)
        end

        context 'when the read preference and max staleness combination is invalid' do

          context 'when max staleness is combined with read preference mode primary' do

            let(:options) do
              'readPreference=primary&maxStalenessSeconds=120'
            end

            it 'raises an exception when read preference is accessed on the client' do
              expect {
                client.server_selector
              }.to raise_exception(Mongo::Error::InvalidServerPreference)
            end
          end

          context 'when the max staleness value is too small' do

            let(:options) do
              'readPreference=secondary&maxStalenessSeconds=89'
            end

            it 'does not raise an exception until the read preference is used' do
              expect(client.read_preference).to eq(BSON::Document.new(mode: :secondary, max_staleness: 89))
            end
          end
        end
      end

      context 'replica set option provided' do
        let(:rs_name) { TEST_SET }
        let(:options) { "replicaSet=#{rs_name}" }

        it 'sets the replica set option' do
          expect(uri.uri_options[:replica_set]).to eq(rs_name)
        end

        it 'sets the options on a client created with the uri' do
          expect(client.options[:replica_set]).to eq(rs_name)
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

          it 'sets the options on a client created with the uri' do
            expect(client.options[:auth_mech]).to eq(expected)
          end

          it 'is case-insensitive' do
            expect(new_local_client(string.downcase).options[:auth_mech]).to eq(expected)
          end
        end

        context 'mongodb-cr' do
          let(:mechanism) { 'MONGODB-CR' }
          let(:expected) { :mongodb_cr }

          it 'sets the auth mechanism to :mongodb_cr' do
            expect(uri.uri_options[:auth_mech]).to eq(expected)
          end

          it 'sets the options on a client created with the uri' do
            expect(client.options[:auth_mech]).to eq(expected)
          end

          it 'is case-insensitive' do
            expect(new_local_client(string.downcase).options[:auth_mech]).to eq(expected)
          end
        end

        context 'gssapi' do
          let(:mechanism) { 'GSSAPI' }
          let(:expected) { :gssapi }

          it 'sets the auth mechanism to :gssapi' do
            expect(uri.uri_options[:auth_mech]).to eq(expected)
          end

          it 'sets the options on a client created with the uri' do
            expect(client.options[:auth_mech]).to eq(expected)
          end

          it 'is case-insensitive' do
            expect(new_local_client(string.downcase).options[:auth_mech]).to eq(expected)
          end
        end

        context 'scram-sha-1' do
          let(:mechanism) { 'SCRAM-SHA-1' }
          let(:expected) { :scram }

          it 'sets the auth mechanism to :scram' do
            expect(uri.uri_options[:auth_mech]).to eq(expected)
          end

          it 'sets the options on a client created with the uri' do
            expect(client.options[:auth_mech]).to eq(expected)
          end

          it 'is case-insensitive' do
            expect(new_local_client(string.downcase).options[:auth_mech]).to eq(expected)
          end
        end

        context 'mongodb-x509' do
          let(:mechanism) { 'MONGODB-X509' }
          let(:expected) { :mongodb_x509 }

          it 'sets the auth mechanism to :mongodb_x509' do
            expect(uri.uri_options[:auth_mech]).to eq(expected)
          end

          it 'sets the options on a client created with the uri' do
            expect(client.options[:auth_mech]).to eq(expected)
          end

          it 'is case-insensitive' do
            expect(new_local_client(string.downcase).options[:auth_mech]).to eq(expected)
          end

          context 'when a username is not provided' do

            it 'recognizes the mechanism with no username' do
              expect(new_local_client(string.downcase).options[:auth_mech]).to eq(expected)
              expect(new_local_client(string.downcase).options[:user]).to be_nil
            end
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
            expect(client.options[:auth_source]).to eq(source)
          end
        end

        context '$external' do
          let(:source) { '$external' }
          let(:expected) { :external }

          it 'sets the auth source to :external' do
            expect(uri.uri_options[:auth_source]).to eq(expected)
          end

          it 'sets the options on a client created with the uri' do
            expect(client.options[:auth_source]).to eq(expected)
          end
        end
      end

      context 'auth mechanism properties provided' do

        context 'service_name' do
          let(:options) do
            "authMechanismProperties=SERVICE_NAME:#{service_name}"
          end

          let(:service_name) { 'foo' }
          let(:expected) { Mongo::Options::Redacted.new({ service_name: service_name }) }

          it 'sets the auth mechanism properties' do
            expect(uri.uri_options[:auth_mech_properties]).to eq(expected)
          end

          it 'sets the options on a client created with the uri' do
            expect(client.options[:auth_mech_properties]).to eq(expected)
          end
        end

        context 'canonicalize_host_name' do
          let(:options) do
            "authMechanismProperties=CANONICALIZE_HOST_NAME:#{canonicalize_host_name}"
          end
          let(:canonicalize_host_name) { 'true' }
          let(:expected) { Mongo::Options::Redacted.new({ canonicalize_host_name: true }) }

          it 'sets the auth mechanism properties' do
            expect(uri.uri_options[:auth_mech_properties]).to eq(expected)
          end

          it 'sets the options on a client created with the uri' do
            expect(client.options[:auth_mech_properties]).to eq(expected)
          end
        end

        context 'service_realm' do
          let(:options) do
            "authMechanismProperties=SERVICE_REALM:#{service_realm}"
          end

          let(:service_realm) { 'dumdum' }
          let(:expected) { Mongo::Options::Redacted.new({ service_realm: service_realm }) }


          it 'sets the auth mechanism properties' do
            expect(uri.uri_options[:auth_mech_properties]).to eq(expected)
          end

          it 'sets the options on a client created with the uri' do
            expect(client.options[:auth_mech_properties]).to eq(expected)
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

          let(:expected) do
            Mongo::Options::Redacted.new({ service_name: service_name,
                                           canonicalize_host_name: true,
                                           service_realm: service_realm })
          end

          it 'sets the auth mechanism properties' do
            expect(uri.uri_options[:auth_mech_properties]).to eq(expected)
          end

          it 'sets the options on a client created with the uri' do
            expect(client.options[:auth_mech_properties]).to eq(expected)
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

      context 'when an app name option is provided' do
        let(:options) { "appname=srv_test" }

        it 'sets the app name on the client' do
          expect(client.options[:app_name]).to eq(:srv_test)
        end
      end

      context 'when a supported compressors option is provided' do
        let(:options) { "compressors=zlib" }

        it 'sets the compressors as an array on the client' do
          expect(client.options[:compressors]).to eq(['zlib'])
        end
      end

      context 'when a non-supported compressors option is provided' do
        let(:options) { "compressors=snoopy" }

        it 'sets no compressors on the client and warns' do
          expect(Mongo::Logger.logger).to receive(:warn)
          expect(client.options[:compressors]).to be_nil
        end
      end

      context 'when a zlibCompressionLevel option is provided' do
        let(:options) { "zlibCompressionLevel=6" }

        it 'sets the zlib compression level on the client' do
          expect(client.options[:zlib_compression_level]).to eq(6)
        end
      end
    end
  end
end
