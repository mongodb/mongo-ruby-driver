# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Auth::Oidc::MachineWorkflow::K8sCallback do
  let(:callback) do
    described_class.new()
  end

  describe '#execute' do
    context 'when using AWS EKS' do
      let(:path) do
        '/path/to/file'
      end

      before do
        allow(ENV).to receive(:[]).with('AWS_WEB_IDENTITY_TOKEN_FILE').and_return(path)
        allow(ENV).to receive(:[]).with('AZURE_FEDERATED_TOKEN_FILE').and_return(nil)
        allow(File).to receive(:read).with(path).and_return('token')
      end

      let(:result) do
        callback.execute(timeout: 50, version: 1)
      end

      it 'returns the token' do
        expect(result[:access_token]).to eq('token')
      end
    end

    context 'when using Azure AKS' do
      let(:path) do
        '/path/to/file'
      end

      before do
        allow(ENV).to receive(:[]).with('AWS_WEB_IDENTITY_TOKEN_FILE').and_return(nil)
        allow(ENV).to receive(:[]).with('AZURE_FEDERATED_TOKEN_FILE').and_return(path)
        allow(File).to receive(:read).with(path).and_return('token')
      end

      let(:result) do
        callback.execute(timeout: 50, version: 1)
      end

      it 'returns the token' do
        expect(result[:access_token]).to eq('token')
      end
    end

    context 'when a token path is not provided' do
      let(:path) do
        '/var/run/secrets/kubernetes.io/serviceaccount/token'
      end

      before do
        allow(ENV).to receive(:[]).with('AWS_WEB_IDENTITY_TOKEN_FILE').and_return(nil)
        allow(ENV).to receive(:[]).with('AZURE_FEDERATED_TOKEN_FILE').and_return(nil)
        allow(File).to receive(:read).with(path).and_return('token')
      end

      let(:result) do
        callback.execute(timeout: 50, version: 1)
      end

      it 'returns the token from the default path' do
        expect(result[:access_token]).to eq('token')
      end
    end
  end
end
