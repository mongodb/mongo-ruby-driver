# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'
require 'support/shared/auth_context'

describe Mongo::Auth::Scram do
  require_no_external_user

  let(:server) do
    authorized_client.cluster.next_primary
  end

  include_context 'auth unit tests'

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

  shared_examples_for 'works correctly' do

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
            auth_mech: auth_mech,
          )
        end

        let(:authenticator) do
          described_class.new(user, connection)
        end

        it 'raises an exception' do
          expect do
            authenticator.login
          end.to raise_error(Mongo::Auth::Unauthorized)
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
          described_class.new(test_user, connection)
        end

        let(:login) do
          authenticator.login
        end

        it 'logs the user into the connection' do
          expect(login['ok']).to eq(1)
        end

        it_behaves_like 'caches scram credentials', :salted_password
        it_behaves_like 'caches scram credentials', :client_key
        it_behaves_like 'caches scram credentials', :server_key

        context 'if conversation has not verified server signature' do
          it 'raises an exception' do
            expect_any_instance_of(Mongo::Auth::ScramConversationBase).to receive(:server_verified?).and_return(false)
            lambda do
              login
            end.should raise_error(Mongo::Error::MissingScramServerSignature)
          end
        end
      end
    end
  end

  context 'when SCRAM-SHA-1 is used' do
    min_server_fcv '3.0'

    let(:auth_mech) { :scram }

    it_behaves_like 'works correctly'
  end

  context 'when SCRAM-SHA-256 is used' do
    min_server_fcv '4.0'

    let(:auth_mech) { :scram256 }

    it_behaves_like 'works correctly'
  end
end
