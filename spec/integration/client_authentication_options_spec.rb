require 'spec_helper'

describe 'Client authentication options' do
  let(:uri) { "mongodb://#{credentials}127.0.0.1:27017/#{options}" }

  let(:credentials) { nil }
  let(:options) { nil }

  let(:client_opts) { {} }

  let(:client) { new_local_client_nmio(uri, client_opts) }

  let(:user) { 'username' }
  let(:pwd) { 'password' }

  shared_examples_for 'a supported auth mechanism' do
    context 'with URI options' do
      let(:credentials) { "#{user}:#{pwd}@" }
      let(:options) { "?authMechanism=#{auth_mech_string}" }

      it 'creates a client with the correct auth mechanism' do
        expect(client.options[:auth_mech]).to eq(auth_mech_sym)
      end
    end

    context 'with client options' do
      let(:client_opts) do
        {
          auth_mech: auth_mech_sym,
          user: user,
          password: pwd,
        }
      end

      it 'creates a client with the correct auth mechanism' do
        expect(client.options[:auth_mech]).to eq(auth_mech_sym)
      end
    end
  end

  shared_examples_for 'auth mechanism that uses database or default auth source' do |default_auth_source|
    context 'where no database is provided' do
      context 'with URI options' do
        let(:credentials) { "#{user}:#{pwd}@" }
        let(:options) { "?authMechanism=#{auth_mech_string}" }

        it 'creates a client with default auth source' do
          expect(client.options['auth_source']).to eq(default_auth_source)
        end
      end

      context 'with client options' do
        let(:client_opts) do
          {
            auth_mech: auth_mech_sym,
            user: user,
            password: pwd,
          }
        end

        it 'creates a client with default auth source' do
          expect(client.options['auth_source']).to eq(default_auth_source)
        end
      end
    end

    context 'where database is provided' do
      let(:database) { 'test-db' }

      context 'with URI options' do
        let(:credentials) { "#{user}:#{pwd}@" }
        let(:options) { "#{database}?authMechanism=#{auth_mech_string}" }

        it 'creates a client with database as auth source' do
          expect(client.options['auth_source']).to eq(database)
        end
      end

      context 'with client options' do
        let(:client_opts) do
          {
            auth_mech: auth_mech_sym,
            user: user,
            password: pwd,
            database: database
          }
        end

        it 'creates a client with database as auth source' do
          expect(client.options['auth_source']).to eq(database)
        end
      end
    end
  end

  shared_examples_for 'an auth mechanism with ssl' do
    let(:ca_file_path) { '/path/to/ca.pem' }
    let(:cert_path) { '/path/to/client.pem' }

    context 'with URI options' do
      let(:credentials) { "#{user}:#{pwd}@" }
      let(:options) { "?authMechanism=#{auth_mech_string}&tls=true&tlsCAFile=#{ca_file_path}&tlsCertificateKeyFile=#{cert_path}" }

      it 'creates a client with ssl properties' do
        expect(client.options[:ssl]).to be true
        expect(client.options[:ssl_cert]).to eq(cert_path)
        expect(client.options[:ssl_ca_cert]).to eq(ca_file_path)
        expect(client.options[:ssl_key]).to eq(cert_path)
      end
    end

    context 'with client options' do
      let(:client_opts) do
        {
          auth_mech: auth_mech_sym,
          ssl: true,
          ssl_cert: cert_path,
          ssl_key: cert_path,
          ssl_ca_cert: ca_file_path,
          user: user,
          password: pwd
        }
      end

      it 'creates a client with ssl properties' do
        expect(client.options[:ssl]).to be true
        expect(client.options[:ssl_cert]).to eq(cert_path)
        expect(client.options[:ssl_ca_cert]).to eq(ca_file_path)
        expect(client.options[:ssl_key]).to eq(cert_path)
      end
    end
  end

  shared_examples_for 'an auth mechanism that doesn\'t support auth_mech_properties' do
    context 'with URI options' do
      let(:credentials) { "#{user}:#{pwd}@" }
      let(:options) { "?authMechanism=#{auth_mech_string}&authMechanismProperties=CANONICALIZE_HOST_NAME:true" }

      it 'raises an exception on client creation' do
        expect {
          client
        }.to raise_error(Mongo::Auth::InvalidConfiguration, /mechanism_properties are not supported/)
      end
    end

    context 'with client options' do
      let(:client_opts) do
        {
          auth_mech: auth_mech_sym,
          user: user,
          password: pwd,
          auth_mech_properties: {
            canonicalize_host_name: true
          }
        }
      end

      it 'raises an exception on client creation' do
        expect {
          client
        }.to raise_error(Mongo::Auth::InvalidConfiguration, /mechanism_properties are not supported/)
      end
    end
  end

  shared_examples_for 'an auth mechanism that doesn\'t support invalid auth sources' do
    context 'with URI options' do
      let(:credentials) { "#{user}:#{pwd}@" }
      let(:options) { "?authMechanism=#{auth_mech_string}&authSource=foo" }

      it 'raises an exception on client creation' do
        expect {
          client
        }.to raise_error(Mongo::Auth::InvalidConfiguration, /invalid auth source/)
      end
    end

    context 'with client options' do
      let(:client_opts) do
        {
          auth_mech: auth_mech_sym,
          user: user,
          password: pwd,
          auth_source: 'foo'
        }
      end

      it 'raises an exception on client creation' do
        expect {
          client
        }.to raise_error(Mongo::Auth::InvalidConfiguration, /invalid auth source/)
      end
    end
  end

  context 'with MONGODB-CR auth mechanism' do
    let(:auth_mech_string) { 'MONGODB-CR' }
    let(:auth_mech_sym) { :mongodb_cr }

    it_behaves_like 'a supported auth mechanism'
    it_behaves_like 'auth mechanism that uses database or default auth source', 'admin'
    it_behaves_like 'an auth mechanism that doesn\'t support auth_mech_properties'
  end

  context 'with SCRAM-SHA-1 auth mechanism' do
    let(:auth_mech_string) { 'SCRAM-SHA-1' }
    let(:auth_mech_sym) { :scram }

    it_behaves_like 'a supported auth mechanism'
    it_behaves_like 'auth mechanism that uses database or default auth source', 'admin'
    it_behaves_like 'an auth mechanism that doesn\'t support auth_mech_properties'
  end

  context 'with SCRAM-SHA-256 auth mechanism' do
    let(:auth_mech_string) { 'SCRAM-SHA-256' }
    let(:auth_mech_sym) { :scram256 }

    it_behaves_like 'a supported auth mechanism'
    it_behaves_like 'auth mechanism that uses database or default auth source', 'admin'
    it_behaves_like 'an auth mechanism that doesn\'t support auth_mech_properties'
  end

  context 'with GSSAPI auth mechanism' do
    require_mongo_kerberos

    let(:auth_mech_string) { 'GSSAPI' }
    let(:auth_mech_sym) { :gssapi }

    it_behaves_like 'a supported auth mechanism'
    it_behaves_like 'an auth mechanism that doesn\'t support invalid auth sources'

    let(:auth_mech_properties) { { canonicalize_host_name: true, service_name: 'other'} }

    context 'with URI options' do
      let(:credentials) { "#{user}:#{pwd}@" }

      context 'with default auth mech properties' do
        let(:options) { '?authMechanism=GSSAPI' }

        it 'correctly sets client options' do
          expect(client.options[:auth_mech_properties]).to eq({ 'service_name' => 'mongodb' })
        end
      end
    end

    context 'with client options' do
      let(:client_opts) do
        {
          auth_mech: :gssapi,
          user: user,
          password: pwd
        }
      end

      it 'sets default auth mech properties' do
        expect(client.options[:auth_mech_properties]).to eq({ 'service_name' => 'mongodb' })
      end
    end
  end

  context 'with PLAIN auth mechanism' do
    let(:auth_mech_string) { 'PLAIN' }
    let(:auth_mech_sym) { :plain }

    it_behaves_like 'a supported auth mechanism'
    it_behaves_like 'auth mechanism that uses database or default auth source', '$external'
    it_behaves_like 'an auth mechanism with ssl'
    it_behaves_like 'an auth mechanism that doesn\'t support auth_mech_properties'
  end

  context 'with MONGODB-X509 auth mechanism' do
    let(:auth_mech_string) { 'MONGODB-X509' }
    let(:auth_mech_sym) { :mongodb_x509 }

    let(:pwd) { nil }

    it_behaves_like 'a supported auth mechanism'
    it_behaves_like 'an auth mechanism with ssl'
    it_behaves_like 'an auth mechanism that doesn\'t support auth_mech_properties'
    it_behaves_like 'an auth mechanism that doesn\'t support invalid auth sources'

    context 'with URI options' do
      let(:credentials) { "#{user}@" }
      let(:options) { '?authMechanism=MONGODB-X509' }

      it 'sets default auth source' do
        expect(client.options[:auth_source]).to eq('$external')
      end

      context 'when username is not provided' do
        let(:credentials) { '' }

        it 'recognizes the mechanism with no username' do
          expect(client.options[:user]).to be_nil
        end
      end

      context 'when a password is provided' do
        let(:credentials) { "#{user}:password@" }

        it 'raises an exception on client creation' do
          expect {
            client
          }.to raise_error(Mongo::Auth::InvalidConfiguration, /password is not supported/)
        end
      end
    end

    context 'with client options' do
      let(:client_opts) { { auth_mech: :mongodb_x509, user: user } }

      it 'sets default auth source' do
        expect(client.options[:auth_source]).to eq('$external')
      end

      context 'when username is not provided' do
        let(:client_opts) { { auth_mech: :mongodb_x509} }

        it 'recognizes the mechanism with no username' do
          expect(client.options[:user]).to be_nil
        end
      end

      context 'when a password is provided' do
        let(:client_opts) { { auth_mech: :mongodb_x509, user: user, password: 'password' } }

        it 'raises an exception on client creation' do
          expect {
            client
          }.to raise_error(Mongo::Auth::InvalidConfiguration, /password is not supported/)
        end
      end
    end
  end

  context 'with no auth mechanism provided' do
    context 'with URI options' do
      context 'with no credentials' do
        it 'creates a client without credentials' do
          expect(client.options[:user]).to be_nil
          expect(client.options[:password]).to be_nil
        end
      end

      context 'with empty username' do
        let(:credentials) { '@' }

        it 'raises an exception' do
          expect {
            client
          }.to raise_error(Mongo::Auth::InvalidConfiguration, /empty username is not supported/)
        end
      end
    end

    context 'with client options' do
      context 'with no credentials' do
        it 'creates a client without credentials' do
          expect(client.options[:user]).to be_nil
          expect(client.options[:password]).to be_nil
        end
      end

      context 'with empty username' do
        let(:client_opts) { { user: '', password: '' } }

        it 'raises an exception' do
          expect {
            client
          }.to raise_error(Mongo::Auth::InvalidConfiguration, /empty username is not supported/)
        end
      end
    end
  end

  context 'with auth source provided' do
    let(:auth_source) { 'foo' }

    context 'with URI options' do
      let(:options) { "?authSource=#{auth_source}" }

      it 'correctly sets auth source on the client' do
        expect(client.options[:auth_source]).to eq(auth_source)
      end
    end

    context 'with client options' do
      let(:client_opts) { { auth_source: auth_source } }

      it 'correctly sets auth source on the client' do
        expect(client.options[:auth_source]).to eq(auth_source)
      end
    end
  end

  context 'with auth mechanism properties' do
    let(:service_name) { 'service name' }
    let(:canonicalize_host_name) { true }
    let(:service_realm) { 'service_realm' }

    let(:auth_mechanism_properties) do
      {
        service_name: service_name,
        canonicalize_host_name: canonicalize_host_name,
        service_realm: service_realm
      }
    end

    context 'with URI options' do
      let(:options) do
        "?authMechanismProperties=SERVICE_NAME:#{service_name}," +
          "CANONICALIZE_HOST_NAME:#{canonicalize_host_name}," +
          "SERVICE_REALM:#{service_realm}"
      end

      it 'correctly sets auth mechanism properties on the client' do
        expect(client.options[:auth_mech_properties]).to eq({
          'service_name' => service_name,
          'canonicalize_host_name' => canonicalize_host_name,
          'service_realm' => service_realm
        })
      end
    end

    context 'with client options' do
      let(:client_opts) { { auth_mech_properties: auth_mechanism_properties } }

      it 'correctly sets auth mechanism properties on the client' do
        expect(client.options[:auth_mech_properties]).to eq({
          'service_name' => service_name,
          'canonicalize_host_name' => canonicalize_host_name,
          'service_realm' => service_realm
        })
      end
    end
  end
end
