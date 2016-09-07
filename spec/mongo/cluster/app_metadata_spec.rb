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
        expect(app_metadata.send(:client_document)[:application][:name]).to eq(:reports)
      end

      context 'when the app name exceeds the max length of 128' do

        let(:cluster) do
          authorized_client.with(app_name: 'x'*129).cluster
        end

        it 'raises an error' do
          expect {
            app_metadata.send(:validate!)
          }.to raise_exception(Mongo::Error::InvalidHandshakeDocument)
        end
      end
    end

    context 'when the cluster does not have an app name option set' do

      it 'does not set the app name' do
        expect(app_metadata.send(:client_document)[:application]).to be(nil)
      end
    end
  end
end