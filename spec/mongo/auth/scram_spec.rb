require 'spec_helper'

describe Mongo::Auth::SCRAM do
  require_no_x509_auth

  let(:server) do
    authorized_client.cluster.next_primary
  end

  let(:connection) do
    Mongo::Server::Connection.new(server, SpecConfig.instance.test_options)
  end

  let(:cache_mod) { Mongo::Auth::CredentialCache }

  shared_examples_for 'caches scram credentials' do |cache_key|

    it 'caches scram credentials' do
      cache_mod.clear
      expect(cache_mod.store).to be_empty

      expect(login['ok']).to eq(1)

      expect(cache_mod.store).not_to be_empty
      client_key_entry = cache_mod.store.keys.detect do |key|
        key.include?(test_user.password) && key.include?(cache_key)
      end
      expect(client_key_entry).not_to be nil
    end
  end

  context 'when SCRAM-SHA-1 is used' do
    min_server_fcv '3.0'

    before do
      connection.connect!
    end

    describe '#login' do

      context 'when the user is not authorized' do

        let(:user) do
          Mongo::Auth::User.new(
            database: 'driver',
            user: 'notauser',
            password: 'password',
            auth_mech: :scram,
          )
        end

        let(:authenticator) do
          described_class.new(user)
        end

        it 'raises an exception' do
          expect {
            authenticator.login(connection)
          }.to raise_error(Mongo::Auth::Unauthorized)
        end

        context 'when compression is used' do
          require_compression
          min_server_fcv '3.6'

          it 'does not compress the message' do
            expect(Mongo::Protocol::Compressed).not_to receive(:new)
            expect {
              authenticator.login
            }.to raise_error(Mongo::Auth::Unauthorized)
          end
        end
      end

      context 'when the user is authorized for the database' do

        let(:authenticator) do
          described_class.new(test_user)
        end

        let(:login) do
          authenticator.login(connection).documents[0]
        end

        it 'logs the user into the connection' do
          expect(login['ok']).to eq(1)
        end

        it_behaves_like 'caches scram credentials', :salted_password
        it_behaves_like 'caches scram credentials', :client_key
        it_behaves_like 'caches scram credentials', :server_key
      end
    end
  end

  context 'when SCRAM-SHA-256 is used' do
    min_server_fcv '4.0'

    before do
      connection.connect!
    end

    describe '#login' do

      context 'when the user is not authorized' do

        let(:user) do
          Mongo::Auth::User.new(
            database: 'driver',
            user: 'notauser',
            password: 'password',
            auth_mech: :scram256,
          )
        end

        let(:authenticator) do
          described_class.new(user)
        end

        it 'raises an exception' do
          expect {
            authenticator.login(connection)
          }.to raise_error(Mongo::Auth::Unauthorized)
        end

        context 'when compression is used' do
          require_compression
          min_server_fcv '3.6'

          it 'does not compress the message' do
            expect(Mongo::Protocol::Compressed).not_to receive(:new)
            expect {
              authenticator.login(connection)
            }.to raise_error(Mongo::Auth::Unauthorized)
          end
        end
      end

      context 'when the user is authorized for the database' do

        let(:authenticator) do
          described_class.new(test_user)
        end

        let(:login) do
          authenticator.login(connection).documents[0]
        end

        it 'logs the user into the connection' do
          expect(login['ok']).to eq(1)
        end

        it_behaves_like 'caches scram credentials', :salted_password
        it_behaves_like 'caches scram credentials', :client_key
        it_behaves_like 'caches scram credentials', :server_key
      end
    end
  end
end
