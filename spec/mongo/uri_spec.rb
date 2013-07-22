require 'spec_helper'

describe Mongo::URI do
  let(:scheme) { 'mongodb://' }
  let(:uri) { described_class.new(string) }

  describe '#initialize' do
    context 'string is not uri' do
      let(:string) { 'tyler' }
      it 'raises an error' do
        expect { uri }.to raise_error(Mongo::URI::BadURI)
      end
    end
  end

  describe '#nodes' do
    let(:string) { "#{scheme}#{nodes}" }

    context 'single node' do
      let(:nodes) { 'localhost' }

      it 'returns an array with the parsed node' do
        expect(uri.nodes).to eq([nodes])
      end
    end

    context 'single node with port' do
      let(:nodes) { 'localhost:27017' }

      it 'returns an array with the parsed node' do
        expect(uri.nodes).to eq([nodes])
      end
    end

    context 'numerical ipv4 node' do
      let(:nodes) { '127.0.0.1' }

      it 'returns an array with the parsed node' do
        expect(uri.nodes).to eq([nodes])
      end
    end

    context 'numerical ipv6 node' do
      let(:nodes) { '[::1]:27107' }

      it 'returns an array with the parsed node' do
        expect(uri.nodes).to eq([nodes])
      end
    end

    context 'unix socket node' do
      let(:nodes) { '/tmp/mongodb-27017.sock' }

      it 'returns an array with the parsed node' do
        expect(uri.nodes).to eq([nodes])
      end
    end

    context 'multiple nodes' do
      let(:nodes) { 'localhost,127.0.0.1' }

      it 'returns an array with the parsed nodes' do
        expect(uri.nodes).to eq(nodes.split(','))
      end
    end

    context 'multiple nodes with ports' do
      let(:nodes) { '127.0.0.1:27107,localhost:27018' }

      it 'returns an array with the parsed nodes' do
        expect(uri.nodes).to eq(nodes.split(','))
      end
    end
  end

  describe '#credentials' do
    let(:nodes)    { 'localhost' }
    let(:string)   { "#{scheme}#{credentials}@#{nodes}" }
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
    let(:nodes)  { 'localhost' }
    let(:string) { "#{scheme}#{nodes}/#{db}" }
    let(:db)     { TEST_DB }

    context 'database provided' do
      it 'returns the database name' do
        expect(uri.database).to eq(TEST_DB)
      end
    end
  end

  describe '#options' do
    let(:nodes)  { 'localhost' }
    let(:string) { "#{scheme}#{nodes}/?#{options}" }

    context 'when no options were provided' do
      let(:string) { "#{scheme}#{nodes}" }

      it 'returns an empty hash' do
        expect(uri.options).to be_empty
      end
    end

    context 'write concern options provided' do

      context 'numerical w value' do
        let(:options) { 'w=1' }
        let(:concern) { { :w => 1 } }

        it 'sets the write concern options' do
          expect(uri.options[:write]).to eq(concern)
        end
      end

      context 'w=majority' do
        let(:options) { 'w=majority' }
        let(:concern) { { :w => :majority } }

        it 'sets the write concern options' do
          expect(uri.options[:write]).to eq(concern)
        end
      end

      context 'journal' do
        let(:options) { 'j=true' }
        let(:concern) { { :j => true } }

        it 'sets the write concern options' do
          expect(uri.options[:write]).to eq(concern)
        end
      end

      context 'fsync' do
        let(:options) { 'fsync=true' }
        let(:concern) { { :fsync => true } }

        it 'sets the write concern options' do
          expect(uri.options[:write]).to eq(concern)
        end
      end

      context 'wtimeoutMS' do
        let(:timeout) { 1234 }
        let(:options) { "w=2&wtimeoutMS=#{timeout}" }
        let(:concern) { { :w => 2, :timeout => timeout } }

        it 'sets the write concern options' do
          expect(uri.options[:write]).to eq(concern)
        end
      end
    end

    context 'read preference option provided' do
      let(:options) { "readPreference=#{mode}" }

      context 'primary' do
        let(:mode) { 'primary' }
        let(:read) { { :mode => :primary } }

        it 'sets the read preference' do
          expect(uri.options[:read]).to eq(read)
        end
      end

      context 'primaryPreferred' do
        let(:mode) { 'primaryPreferred' }
        let(:read) { { :mode => :primary_preferred } }

        it 'sets the read preference' do
          expect(uri.options[:read]).to eq(read)
        end
      end

      context 'secondary' do
        let(:mode) { 'secondary' }
        let(:read) { { :mode => :secondary } }

        it 'sets the read preference' do
          expect(uri.options[:read]).to eq(read)
        end
      end

      context 'secondaryPreferred' do
        let(:mode) { 'secondaryPreferred' }
        let(:read) { { :mode => :secondary_preferred } }

        it 'sets the read preference' do
          expect(uri.options[:read]).to eq(read)
        end
      end

      context 'nearest' do
        let(:mode) { 'nearest' }
        let(:read) { { :mode => :nearest } }

        it 'sets the read preference' do
          expect(uri.options[:read]).to eq(read)
        end
      end
    end

    context 'read preferece tags provided' do

      context 'single read preference tag set' do
        let(:options) do
          'readPreferenceTags=dc:ny,rack:1'
        end

        let(:read) do
          { :tags => [{ :dc => 'ny', :rack => '1' }] }
        end

        it 'sets the read preference tag set' do
          expect(uri.options[:read]).to eq(read)
        end
      end

      context 'multiple read preference tag sets' do
        let(:options) do
          'readPreferenceTags=dc:ny&readPreferenceTags=dc:bos'
        end

        let(:read) do
          { :tags => [{ :dc => 'ny' }, { :dc => 'bos' }] }
        end

        it 'sets the read preference tag sets' do
          expect(uri.options[:read]).to eq(read)
        end
      end
    end

    context 'replica set option provided' do
      let(:rs_name) { TEST_SET }
      let(:options) { "replicaSet=#{rs_name}" }

      it 'sets the replica set option' do
        expect(uri.options[:replica_set]).to eq(rs_name)
      end
    end

    context 'auth mechanism provided' do
      let(:options) { "authMechanism=#{mechanism}" }

      context 'plain' do
        let(:mechanism) { 'PLAIN' }
        let(:auth) { { :mechanism => :plain } }

        it 'sets the auth mechanism to :plain' do
          expect(uri.options[:auth]).to eq(auth)
        end
      end

      context 'mongodb-cr' do
        let(:mechanism) { 'MONGODB-CR' }
        let(:auth) { { :mechanism => :mongodb_cr } }

        it 'sets the auth mechanism to :mongodb_cr' do
          expect(uri.options[:auth]).to eq(auth)
        end
      end

      context 'gssapi' do
        let(:mechanism) { 'GSSAPI' }
        let(:auth) { { :mechanism => :gssapi } }

        it 'sets the auth mechanism to :gssapi' do
          expect(uri.options[:auth]).to eq(auth)
        end
      end
    end

    context 'auth source provided' do
      let(:options) { "authSource=#{source}" }

      context 'regular db' do
        let(:source) { 'foo' }
        let(:auth) { { :source => 'foo' } }

        it 'sets the auth source to the database' do
          expect(uri.options[:auth]).to eq(auth)
        end
      end

      context '$external' do
        let(:source) { '$external' }
        let(:auth) { { :source => :external } }

        it 'sets the auth source to :external' do
          expect(uri.options[:auth]).to eq(auth)
        end
      end
    end

    context 'connectTimeoutMS' do
      let(:timeout) { 4567 }
      let(:options) { "connectTimeoutMS=#{timeout}" }

      it 'sets the the connect timeout' do
        expect(uri.options[:connect_timeout]).to eq(timeout)
      end
    end

    context 'socketTimeoutMS' do
      let(:timeout) { 8910 }
      let(:options) { "socketTimeoutMS=#{timeout}" }

      it 'sets the socket timeout' do
        expect(uri.options[:socket_timeout]).to eq(timeout)
      end
    end

    context 'ssl' do
      let(:options) { "ssl=#{ssl}" }

      context 'true' do
        let(:ssl) { true }

        it 'sets the ssl option to true' do
          expect(uri.options[:ssl]).to be_true
        end
      end

      context 'false' do
        let(:ssl) { false }

        it 'sets the ssl option to false' do
          expect(uri.options[:ssl]).to be_false
        end
      end
    end

    context 'grouped and non-grouped options provided' do
      let(:options) { 'w=1&ssl=true' }

      it 'do not overshadow top level options' do
        expect(uri.options).not_to be_empty
      end
    end
  end
end
