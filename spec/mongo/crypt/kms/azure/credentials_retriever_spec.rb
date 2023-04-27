# frozen_string_literal: true

require 'lite_spec_helper'

describe Mongo::Crypt::KMS::Azure::CredentialsRetriever do
  # The tests here require fake azure server, which is started in FLE
  # configurations on evergreen. If you want to run these tests locally,
  # you need to start the server manually. See .evergreen/run-tests.sh
  # for the command to start the server.
  before do
    skip 'These tests require fake azure server to be running' unless SpecConfig.instance.fle?
  end

  describe '.fetch_access_token' do
    context 'when response is valid' do
      let(:token) do
        described_class.fetch_access_token
      end

      it 'returns access token' do
        expect(token.access_token).to eq('magic-cookie')
      end

      it 'returns expiration time' do
        expect(token.expires_in).to eq(70)
      end
    end

    context 'when response contains empty json' do
      it 'raises error' do
        expect do
          described_class.fetch_access_token('X-MongoDB-HTTP-TestParams' => 'case=empty-json')
        end.to raise_error(Mongo::Crypt::KMS::CredentialsNotFound)
      end
    end

    context 'when response contains invalid json' do
      it 'raises error' do
        expect do
          described_class.fetch_access_token('X-MongoDB-HTTP-TestParams' => 'case=bad-json')
        end.to raise_error(Mongo::Crypt::KMS::CredentialsNotFound)
      end
    end

    context 'when metadata host responds with 500' do
      it 'raises error' do
        expect do
          described_class.fetch_access_token('X-MongoDB-HTTP-TestParams' => 'case=500')
        end.to raise_error(Mongo::Crypt::KMS::CredentialsNotFound)
      end
    end

    context 'when metadata host responds with 404' do
      it 'raises error' do
        expect do
          described_class.fetch_access_token('X-MongoDB-HTTP-TestParams' => 'case=404')
        end.to raise_error(Mongo::Crypt::KMS::CredentialsNotFound)
      end
    end

    context 'when metadata host is slow' do
      it 'raises error' do
        expect do
          described_class.fetch_access_token('X-MongoDB-HTTP-TestParams' => 'case=slow')
        end.to raise_error(Mongo::Crypt::KMS::CredentialsNotFound)
      end
    end
  end
end
