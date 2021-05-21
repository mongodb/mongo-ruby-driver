# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'
require_relative './app_metadata_shared'

describe Mongo::Server::AppMetadata do

  let(:app_metadata) do
    described_class.new(cluster.options)
  end

  let(:cluster) do
    authorized_client.cluster
  end

  describe '#initialize' do

    context 'when the cluster has an app name option set' do

      let(:client) do
        authorized_client.with(app_name: :app_metadata_test)
      end

      let(:cluster) do
        client.cluster
      end

      it 'sets the app name' do
        expect(app_metadata.send(:full_client_document)[:application][:name]).to eq('app_metadata_test')
      end

      context 'when the app name exceeds the max length of 128' do

        let(:client) do
          authorized_client.with(app_name: "\u3042"*43)
        end

        let(:cluster) do
          client.cluster
        end

        it 'raises an error' do
          expect {
            app_metadata.send(:validate!)
          }.to raise_exception(Mongo::Error::InvalidApplicationName)
        end
      end
    end

    context 'when the cluster does not have an app name option set' do

      it 'does not set the app name' do
        expect(app_metadata.send(:full_client_document)[:application]).to be(nil)
      end
    end

    context 'when the client document exceeds the max of 512 bytes' do
      # Server api parameters change metadata length
      require_no_required_api_version

      context 'when the os.type length is too long' do

        before do
          allow(app_metadata).to receive(:type).and_return('x'*500)
        end

        it 'truncates the document' do
          expect(
            app_metadata.validated_document.to_bson.to_s.size
          ).to be < described_class::MAX_DOCUMENT_SIZE
        end
      end

      context 'when the os.name length is too long' do

        before do
          allow(app_metadata).to receive(:name).and_return('x'*500)
        end

        it 'truncates the document' do
          expect(
            app_metadata.validated_document.to_bson.to_s.size
          ).to be < described_class::MAX_DOCUMENT_SIZE
        end
      end

      context 'when the os.architecture length is too long' do

        before do
          allow(app_metadata).to receive(:architecture).and_return('x'*500)
        end

        it 'truncates the document' do
          expect(
            app_metadata.validated_document.to_bson.to_s.size
          ).to be < described_class::MAX_DOCUMENT_SIZE
        end
      end

      context 'when the platform length is too long' do

        before do
          allow(app_metadata).to receive(:platform).and_return('x'*500)
        end

        it 'truncates the document' do
          expect(
            app_metadata.validated_document.to_bson.to_s.size
          ).to be < described_class::MAX_DOCUMENT_SIZE
        end
      end

      context 'when the driver info is too long' do
        require_no_compression

        before do
          allow(app_metadata).to receive(:driver_doc).and_return('x'*500)
        end

        it 'truncates the document' do
          expect(
            app_metadata.validated_document.to_bson.to_s.size
          ).to be < described_class::MAX_DOCUMENT_SIZE
        end
      end
    end
  end

  describe '#document' do
    let(:document) do
      app_metadata.send(:document)
    end

    context 'when user is given and auth_mech is not given' do
      let(:app_metadata) do
        described_class.new(user: 'foo')
      end

      it 'includes saslSupportedMechs' do
        expect(document[:saslSupportedMechs]).to eq('admin.foo')
      end
    end

    it_behaves_like 'app metadata document'
  end
end
