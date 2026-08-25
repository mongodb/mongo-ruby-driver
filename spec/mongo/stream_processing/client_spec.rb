# frozen_string_literal: true

require 'lite_spec_helper'

describe Mongo::StreamProcessing::Client do
  describe '.workspace_uri?' do
    context 'with a production workspace URI' do
      it 'returns true' do
        uri = 'mongodb://atlas-stream-699c842ef433fe6001480b17-etif1.virginia-usa.a.query.mongodb.net/'
        expect(described_class.workspace_uri?(uri)).to be true
      end
    end

    context 'with a workspace URI that includes credentials and port' do
      it 'returns true' do
        uri = 'mongodb://user:pass@atlas-stream-xyz.us-east-1.a.query.mongodb.net:27017/?retryWrites=true'
        expect(described_class.workspace_uri?(uri)).to be true
      end
    end

    context 'with a staging workspace URI (mongodb-stage.net)' do
      it 'returns true' do
        uri = 'mongodb://user:pass@atlas-stream-699c842ef433fe6001480b17-etif1.virginia-usa.a.query.mongodb-stage.net'
        expect(described_class.workspace_uri?(uri)).to be true
      end
    end

    context 'with a URI using uppercase scheme' do
      it 'returns true' do
        uri = 'MONGODB://atlas-stream-xyz.us-east-1.a.query.mongodb.net/'
        expect(described_class.workspace_uri?(uri)).to be true
      end
    end

    context 'with a standard cluster URI' do
      it 'returns false' do
        expect(described_class.workspace_uri?('mongodb://localhost:27017/')).to be false
      end
    end

    context 'with an SRV URI' do
      it 'returns false' do
        expect(described_class.workspace_uri?('mongodb+srv://cluster0.example.mongodb.net/')).to be false
      end
    end

    context 'with a URI missing the atlas-stream- prefix' do
      it 'returns false' do
        expect(described_class.workspace_uri?('mongodb://abc.virginia-usa.a.query.mongodb.net/')).to be false
      end
    end

    context 'with a hostname that contains atlas-stream- but the wrong TLD' do
      it 'returns false' do
        expect(described_class.workspace_uri?('mongodb://atlas-stream-x.example.com/')).to be false
      end
    end

    context 'with a non-string argument' do
      it 'returns false' do
        expect(described_class.workspace_uri?(nil)).to be false
        expect(described_class.workspace_uri?(123)).to be false
      end
    end
  end

  describe '#initialize' do
    let(:workspace_uri) { 'mongodb://atlas-stream-x.us-east-1.a.query.mongodb.net/' }

    context 'with a non-workspace URI' do
      it 'raises ArgumentError' do
        expect do
          described_class.new('mongodb://localhost:27017/')
        end.to raise_error(ArgumentError, /workspace endpoint URI/)
      end
    end

    context 'with an SRV URI' do
      it 'raises ArgumentError' do
        # NOTE: `workspace_uri?` already rejects the SRV scheme because the
        # scheme check is `mongodb://` only; this still surfaces with the
        # workspace-URI error message.
        expect do
          described_class.new('mongodb+srv://atlas-stream-x.us-east-1.a.query.mongodb.net/')
        end.to raise_error(ArgumentError, /workspace endpoint URI/)
      end
    end

    context 'with ssl: false' do
      it 'raises ArgumentError' do
        expect do
          described_class.new(workspace_uri, ssl: false)
        end.to raise_error(ArgumentError, /TLS cannot be disabled/)
      end
    end
  end
end
