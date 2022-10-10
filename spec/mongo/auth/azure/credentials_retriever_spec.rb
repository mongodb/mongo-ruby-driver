# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'

describe Mongo::Auth::Azure::CredentialsRetriever do
  # This spec requires face Azure IMDS endpoint, which is available only
  # in our FLE test configurations.
  require_libmongocrypt
  require_enterprise
  min_server_fcv '4.2'
  describe '#credentials' do
    let(:imds_host) do
      'localhost'
    end

    let(:imds_port) do
      8080
    end

    let(:headers) do
      {}
    end

    let(:fake_token) do
      # This value is returned by the fake Azure endpoint we use to test.
      'magic-cookie'
    end

    let(:subject) do
      described_class.new(imds_host, imds_port, headers)
    end

    context 'when IMDS responds with the token' do
      it 'returns Azure credentials' do
        expect(subject.credentials.to_h).to eq({'accessToken' => fake_token})
      end
    end


    context 'when empty json returned' do
      let(:headers) do
        {
          'X-MongoDB-HTTP-TestParams' => 'case=empty-json'
        }
      end

      it 'raises an error' do
        expect do
          subject.credentials
        end.to raise_error Mongo::Auth::Azure::CredentialsNotFound
      end
    end

    context 'when bad json returned' do
      let(:headers) do
        {
          'X-MongoDB-HTTP-TestParams' => 'case=bad-json'
        }
      end

      it 'raises an error' do
        expect do
          subject.credentials
        end.to raise_error Mongo::Auth::Azure::CredentialsNotFound
      end
    end

    context 'when credentials not found' do
      let(:headers) do
        {
          'X-MongoDB-HTTP-TestParams' => 'case=404'
        }
      end

      it 'raises an error' do
        expect do
          subject.credentials
        end.to raise_error Mongo::Auth::Azure::CredentialsNotFound
      end
    end

    context 'when internal server error' do
      let(:headers) do
        {
          'X-MongoDB-HTTP-TestParams' => 'case=500'
        }
      end

      it 'raises an error' do
        expect do
          subject.credentials
        end.to raise_error Mongo::Auth::Azure::CredentialsNotFound
      end
    end

    context 'when server is slow' do
      let(:headers) do
        {
          'X-MongoDB-HTTP-TestParams' => 'case=slow-response'
        }
      end

      it 'raises an error' do
        expect do
          subject.credentials
        end.to raise_error Mongo::Auth::Azure::CredentialsNotFound
      end
    end

    context 'when the response is too large' do
      let(:headers) do
        {
          'X-MongoDB-HTTP-TestParams' => 'case=giant'
        }
      end

      it 'raises an error' do
        expect do
          subject.credentials
        end.to raise_error Mongo::Auth::Azure::CredentialsNotFound
      end
    end
  end
end
