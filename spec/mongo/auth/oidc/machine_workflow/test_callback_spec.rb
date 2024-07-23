# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Auth::Oidc::MachineWorkflow::TestCallback do
  let(:callback) do
    described_class.new()
  end

  describe '#execute' do
    context 'when a token path is provided' do
      let(:path) do
        '/path/to/file'
      end

      before do
        allow(ENV).to receive(:fetch).with('OIDC_TOKEN_FILE').and_return(path)
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
      it 'raises an error' do
        expect {
          callback.execute(timeout: 50, version: 1)
        }.to raise_error(KeyError)
      end
    end
  end
end
