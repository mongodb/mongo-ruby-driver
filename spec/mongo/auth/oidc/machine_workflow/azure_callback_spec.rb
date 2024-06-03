# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Auth::Oidc::MachineWorkflow::AzureCallback do

  let(:properties) do
    { token_resource: 'audience' }
  end

  let(:callback) do
    described_class.new(auth_mech_properties: properties)
  end

  describe '#execute' do
    context 'when the response is a 200' do
      let(:response) do
        double('response')
      end

      before do
        expect(response).to receive(:code).and_return('200')
        expect(response).to receive(:body).and_return('{ "access_token": "token", "expires_in": 500 }')
        allow(Net::HTTP).to receive(:start).with('169.254.169.254', 80, use_ssl: false).and_return(response)
      end

      let(:result) do
        callback.execute(timeout: 50, version: 1)
      end

      it 'returns the token' do
        expect(result[:access_token]).to eq('token')
      end
    end

    context 'when the response is not a 200' do
      let(:response) do
        double('response')
      end

      before do
        expect(response).to receive(:code).twice.and_return('500')
        allow(Net::HTTP).to receive(:start).with('169.254.169.254', 80, use_ssl: false).and_return(response)
      end

      let(:result) do
      end

      it 'raises an error' do
        expect {
          callback.execute(timeout: 50, version: 1)
        }.to raise_error(Mongo::Error::OidcError)
      end
    end
  end
end
