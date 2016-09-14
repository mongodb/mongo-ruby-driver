require 'spec_helper'

describe Mongo::Cluster::AppMetadata do

  let(:app_metadata) do
    described_class.new(cluster)
  end

  let(:cluster) do
    authorized_client.cluster
  end

  describe '#initialize' do

    context 'when the cluster has an app name option set' do

      let(:cluster) do
        authorized_client.with(app_name: :reports).cluster
      end

      it 'sets the app name' do
        expect(app_metadata.send(:full_client_document)[:application][:name]).to eq(:reports)
      end

      context 'when the app name exceeds the max length of 128' do

        let(:cluster) do
          authorized_client.with(app_name: "\u3042"*43).cluster
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

      context 'when the os.type length is too long' do

        before do
          allow(app_metadata).to receive(:type).and_return('x'*500)
        end

        it 'truncates the document' do
          expect(app_metadata.send(:ismaster_bytes)).to be_a(String)
        end
      end

      context 'when the os.name length is too long' do

        before do
          allow(app_metadata).to receive(:name).and_return('x'*500)
        end

        it 'truncates the document' do
          expect(app_metadata.send(:ismaster_bytes)).to be_a(String)
        end
      end

      context 'when the os.architecture length is too long' do

        before do
          allow(app_metadata).to receive(:architecture).and_return('x'*500)
        end

        it 'truncates the document' do
          expect(app_metadata.send(:ismaster_bytes)).to be_a(String)
        end
      end

      context 'when the platform length is too long' do

        before do
          allow(app_metadata).to receive(:platform).and_return('x'*500)
        end

        it 'truncates the document to be just an ismaster command' do
          expect(app_metadata.send(:ismaster_bytes)).to be_a(String)
        end
      end

      context 'when the driver info is too long' do

        before do
          allow(app_metadata).to receive(:driver_doc).and_return('x'*500)
        end

        it 'truncates the document to be just an ismaster command' do
          expect(app_metadata.ismaster_bytes.length).to eq(Mongo::Server::Monitor::Connection::ISMASTER_BYTES.length)
        end
      end
    end
  end
end