# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::URI::SRVProtocol do
  require_external_connectivity
  clean_slate_for_all_if_possible
  retry_test

  let(:scheme) { 'mongodb+srv://' }
  let(:uri) { described_class.new(string) }

  let(:client) do
    new_local_client_nmio(string)
  end

  shared_examples "roundtrips string" do
    it "returns the correct string for the uri" do
      expect(uri.to_s).to eq(URI::DEFAULT_PARSER.unescape(string))
    end
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

    context 'when the {tld} is empty' do

      let(:string) { "#{scheme}#{hosts}" }
      let(:hosts) { '10gen.cc./' }

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
      let(:string) { "#{scheme}#{servers}#{options}" }
      let(:servers) { 'test1.test.build.10gen.cc' }
      let(:options) { '' }

      context 'single server' do
        let(:servers) { 'test5.test.build.10gen.cc' }
        it 'returns an array with the parsed server' do
          expect(uri.servers).to eq(['localhost.test.build.10gen.cc:27017'])
        end

        include_examples "roundtrips string"
      end

      context 'multiple servers' do
        let(:hosts) { ['localhost.test.build.10gen.cc:27017', 'localhost.test.build.10gen.cc:27018'] }

        context 'without srvMaxHosts' do
          it 'returns an array with the parsed servers' do
            expect(uri.servers.length).to eq 2
            uri.servers.should =~ hosts
          end

          include_examples "roundtrips string"
        end

        context 'with srvMaxHosts' do
          let(:options) { '/?srvMaxHosts=1' }
          it 'returns an array with only one of the parsed servers' do
            expect(uri.servers.length).to eq 1
            expect(hosts.include?(uri.servers.first)).to be true
          end

          include_examples "roundtrips string"
        end

        context 'with srvMaxHosts > total hosts' do
          let(:options) { '/?srvMaxHosts=3' }
          it 'returns an array with only one of the parsed servers' do
            expect(uri.servers.length).to eq 2
            uri.servers.should =~ hosts
          end

          include_examples "roundtrips string"
        end

        context 'with srvMaxHosts == total hosts' do
          let(:options) { '/?srvMaxHosts=2' }
          it 'returns an array with only one of the parsed servers' do
            expect(uri.servers.length).to eq 2
            uri.servers.should =~ hosts
          end

          include_examples "roundtrips string"
        end

        context 'with srvMaxHosts=0' do
          let(:options) { '/?srvMaxHosts=0' }
          it 'returns an array with only one of the parsed servers' do
            expect(uri.servers.length).to eq 2
            uri.servers.should =~ hosts
          end

          include_examples "roundtrips string"
        end

        context 'when setting the srvServiceName' do
          let(:servers) { 'test22.test.build.10gen.cc' }
          let(:options) { '/?srvServiceName=customname' }

          it 'returns an array with the parsed server' do
            uri.servers.should =~ hosts
          end

          include_examples "roundtrips string"
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

      include_examples "roundtrips string"
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

        it "drops the colon in to_s" do
          expect(uri.to_s).to eq("mongodb+srv://tyler@test5.test.build.10gen.cc")
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
      let(:servers)  { 'test5.test.build.10gen.cc' }
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
      let(:servers)  { 'test5.test.build.10gen.cc' }
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
            expect(client.options[:write_concern]).to eq(concern)
          end

          it "roundtrips the string with camelCase" do
            expect(uri.to_s).to eq("mongodb+srv://test5.test.build.10gen.cc/?w=2&wTimeoutMS=1234")
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

          include_examples "roundtrips string"
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

          include_examples "roundtrips string"
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

          include_examples "roundtrips string"
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

          include_examples "roundtrips string"
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
          expect(client.options[:read]).to eq(read)
        end

        it "rountrips the string with lowercase values" do
          expect(uri.to_s).to eq("mongodb+srv://test5.test.build.10gen.cc/?readPreference=secondary&maxStalenessSeconds=120")
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

            it 'does not raise an exception and is omitted' do
              expect(client.read_preference).to eq(BSON::Document.new(mode: :secondary))
            end

            it "drops maxStalenessSeconds in to_s" do
              expect(uri.to_s).to eq("mongodb+srv://test5.test.build.10gen.cc/?readPreference=secondary")
            end
          end
        end
      end

      context 'replica set option provided' do
        let(:rs_name) { 'test-rs-name' }
        let(:options) { "replicaSet=#{rs_name}" }

        it 'sets the replica set option' do
          expect(uri.uri_options[:replica_set]).to eq(rs_name)
        end

        it 'sets the options on a client created with the uri' do
          expect(client.options[:replica_set]).to eq(rs_name)
        end

        include_examples "roundtrips string"
      end

      context 'auth mechanism provided' do
        let(:options)     { "authMechanism=#{mechanism}" }
        let(:string)      { "#{scheme}#{credentials}@#{servers}/?#{options}" }
        let(:user)        { 'tyler' }
        let(:password)    { 's3kr4t' }
        let(:credentials) { "#{user}:#{password}" }

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
            client = new_local_client_nmio(string.downcase)
            expect(client.options[:auth_mech]).to eq(expected)
          end

          include_examples "roundtrips string"
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
            client = new_local_client_nmio(string.downcase)
            expect(client.options[:auth_mech]).to eq(expected)
          end

          include_examples "roundtrips string"
        end

        context 'gssapi' do
          require_mongo_kerberos

          let(:mechanism) { 'GSSAPI' }
          let(:expected)  { :gssapi }
          let(:options)   { "authMechanism=#{mechanism}&authSource=$external" }

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

          it "roundtrips the string" do
            expect(uri.to_s).to eq("mongodb+srv://tyler:s3kr4t@test5.test.build.10gen.cc/?authSource=$external&authMechanism=GSSAPI")
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
            client = new_local_client_nmio(string.downcase)
            expect(client.options[:auth_mech]).to eq(expected)
          end

          include_examples "roundtrips string"
        end

        context 'mongodb-x509' do
          let(:options)     { "authMechanism=#{mechanism}&authSource=$external" }
          let(:mechanism)   { 'MONGODB-X509' }
          let(:expected)    { :mongodb_x509 }
          let(:credentials) { user }

          it 'sets the auth mechanism to :mongodb_x509' do
            expect(uri.uri_options[:auth_mech]).to eq(expected)
          end

          it 'sets the options on a client created with the uri' do
            expect(client.options[:auth_mech]).to eq(expected)
          end

          it 'is case-insensitive' do
              client = new_local_client_nmio(string.downcase)
            expect(client.options[:auth_mech]).to eq(expected)
          end

          it "roundtrips the string" do
            expect(uri.to_s).to eq("mongodb+srv://tyler@test5.test.build.10gen.cc/?authSource=$external&authMechanism=MONGODB-X509")
          end

          context 'when a username is not provided' do
            let(:string) { "#{scheme}#{servers}/?#{options}" }
            it 'recognizes the mechanism with no username' do
              client = new_local_client_nmio(string.downcase)
              expect(client.options[:auth_mech]).to eq(expected)
              expect(client.options[:user]).to be_nil
            end

            it "roundtrips the string" do
              expect(uri.to_s).to eq("mongodb+srv://test5.test.build.10gen.cc/?authSource=$external&authMechanism=MONGODB-X509")
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

          include_examples "roundtrips string"
        end
      end

      # This context exactly duplicates the same one in uri_spec.rb
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

      context 'when providing maxConnecting' do

        let(:max_connecting) { 10 }
        let(:options) { "maxConnecting=#{max_connecting}" }

        it 'sets the max connecting option' do
          expect(uri.uri_options[:max_connecting]).to eq(max_connecting)
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

      context 'when providing waitQueueTimeoutMS' do

        let(:wait_queue_timeout) { 500 }
        let(:options) { "waitQueueTimeoutMS=#{wait_queue_timeout}" }

        it 'sets the wait queue timeout option' do
          expect(uri.uri_options[:wait_queue_timeout]).to eq(0.5)
        end

        include_examples "roundtrips string"
      end

      context 'when providing srvMaxHosts' do
        let(:srv_max_hosts) { 1 }
        let(:options) { "srvMaxHosts=#{srv_max_hosts}" }

        it 'sets the srv max hosts option' do
          expect(uri.uri_options[:srv_max_hosts]).to eq(srv_max_hosts)
        end

        include_examples "roundtrips string"
      end

      context 'when providing srvMaxHosts as 0' do
        let(:srv_max_hosts) { 0 }
        let(:options) { "srvMaxHosts=#{srv_max_hosts}" }

        it 'doesn\'t set the srv max hosts option' do
          expect(uri.uri_options[:srv_max_hosts]).to eq(srv_max_hosts)
        end

        include_examples "roundtrips string"
      end

      context 'when providing invalid integer to srvMaxHosts' do
        let(:srv_max_hosts) { -1 }
        let(:options) { "srvMaxHosts=#{srv_max_hosts}" }

        it 'does not set the srv max hosts option' do
          expect(uri.uri_options).to_not have_key(:srv_max_hosts)
        end

        it "drops srvMaxHosts in to_s" do
          expect(uri.to_s).to eq("mongodb+srv://test5.test.build.10gen.cc")
        end
      end

      context 'when providing invalid type to srvMaxHosts' do
        let(:srv_max_hosts) { "foo" }
        let(:options) { "srvMaxHosts=#{srv_max_hosts}" }

        it 'does not set the srv max hosts option' do
          expect(uri.uri_options).to_not have_key(:srv_max_hosts)
        end

        it "drops srvMaxHosts in to_s" do
          expect(uri.to_s).to eq("mongodb+srv://test5.test.build.10gen.cc")
        end
      end

      context 'when providing srvServiceName' do
        let(:srv_service_name) { "mongodb" }
        let(:options) { "srvServiceName=#{srv_service_name}" }

        it 'sets the srv service name option' do
          expect(uri.uri_options[:srv_service_name]).to eq(srv_service_name)
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

          it "uses tls in to_s" do
            expect(uri.to_s).to eq("mongodb+srv://test5.test.build.10gen.cc/?tls=true")
          end
        end

        context 'false' do
          let(:ssl) { false }

          it 'sets the ssl option to false' do
            expect(uri.uri_options[:ssl]).to be false
          end

          it "uses tls in to_s" do
            expect(uri.to_s).to eq("mongodb+srv://test5.test.build.10gen.cc/?tls=false")
          end
        end
      end

      context 'grouped and non-grouped options provided' do
        let(:options) { 'w=1&ssl=true' }

        it 'do not overshadow top level options' do
          expect(uri.uri_options).not_to be_empty
        end

        it "uses tls in to_s" do
          expect(uri.to_s).to eq("mongodb+srv://test5.test.build.10gen.cc/?w=1&tls=true")
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

        it "drops the invalid option in to_s" do
          expect(uri.to_s).to eq("mongodb+srv://test5.test.build.10gen.cc")
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

          it "drops the invalid option in to_s" do
            expect(uri.to_s).to eq("mongodb+srv://test5.test.build.10gen.cc/?waitQueueTimeoutMS=500&tls=true")
          end
        end
      end

      context 'when an app name option is provided' do
        let(:options) { "appName=srv_test" }

        it 'sets the app name on the client' do
          expect(client.options[:app_name]).to eq('srv_test')
        end

        include_examples "roundtrips string"
      end

      context 'when a supported compressors option is provided' do
        let(:options) { "compressors=zlib" }

        it 'sets the compressors as an array on the client' do
          expect(client.options[:compressors]).to eq(['zlib'])
        end

        include_examples "roundtrips string"
      end

      context 'when a non-supported compressors option is provided' do
        let(:options) { "compressors=snoopy" }

        it 'sets no compressors on the client and warns' do
          expect(Mongo::Logger.logger).to receive(:warn)
          expect(client.options[:compressors]).to be_nil
        end

        include_examples "roundtrips string"
      end

      context 'when a zlibCompressionLevel option is provided' do
        let(:options) { "zlibCompressionLevel=6" }

        it 'sets the zlib compression level on the client' do
          expect(client.options[:zlib_compression_level]).to eq(6)
        end

        include_examples "roundtrips string"
      end
    end
  end

  describe '#validate_srv_hostname' do
    let(:valid_hostname) do
    end

    let(:dummy_uri) do
      Mongo::URI::SRVProtocol.new("mongodb+srv://test1.test.build.10gen.cc/")
    end

    let(:validate) do
      dummy_uri.send(:validate_srv_hostname, hostname)
    end

    context 'when the hostname is valid' do
      let(:hostname) do
        'a.b.c'
      end

      it 'does not raise an error' do
        expect { validate }.not_to raise_error
      end
    end

    context 'when the hostname has a trailing dot' do
      let(:hostname) do
        "a.b.c."
      end

      it 'raises an error' do
        expect { validate }.to raise_error(Mongo::Error::InvalidURI, /Hostname cannot end with a dot: a\.b\.c\./)
      end
    end

    context 'when the hostname is empty' do
      let(:hostname) do
        ''
      end

      it 'raises an error' do
        expect { validate }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'when the hostname has only one part' do
      let(:hostname) do
        'a'
      end

      it 'raises an error' do
        expect { validate }.to raise_error(Mongo::Error::InvalidURI)
      end
    end


    context 'when the hostname has only two parts' do
      let(:hostname) do
        'a.b'
      end

      it 'raises an error' do
        expect { validate }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'when the hostname has an empty last part' do
      let(:hostname) do
        'a.b.'
      end

      it 'it raises an error' do
        expect { validate }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'when multiple hostnames are specified' do
      it 'raises an error' do
        expect do
          Mongo::URI::SRVProtocol.new("mongodb+srv://a.b.c,d.e.f/")
        end.to raise_error(Mongo::Error::InvalidURI, /One and only one host is required/)
      end
    end

    context 'when the hostname contains a colon' do
      let(:hostname) do
        'a.b.c:27017'
      end

      it 'raises an error' do
        expect { validate }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'when the hostname starts with a dot' do
      let(:hostname) do
        '.a.b.c'
      end

      it 'raises an error' do
        expect { validate }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'when the hostname ends with consecutive dots' do
      let(:hostname) do
        'a.b.c..'
      end

      it 'raises an error' do
        expect { validate }.to raise_error(Mongo::Error::InvalidURI)
      end
    end

    context 'when the hostname contains consecutive dots in the middle' do
      let(:hostname) do
        'a..b.c'
      end

      it 'raises an error' do
        expect { validate }.to raise_error(Mongo::Error::InvalidURI)
      end
    end
  end
end
