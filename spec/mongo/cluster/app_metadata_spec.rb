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

      let(:client) do
        authorized_client.with(app_name: :app_metadata_test)
      end

      let(:cluster) do
        client.cluster
      end

      after do
        client.close
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

        after do
          client.close
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

        it 'truncates the document to be just an ismaster command and the compressors', unless: compression_enabled? do
          # Because we sometimes request that the server provide a list of valid auth mechanisms for
          # the user, we need to conditionally add the length of that metadata to the expected
          # length of the isMaster document.
          sasl_supported_mechs_size = 0
          sasl_supported_mechs = app_metadata.instance_variable_get(:@request_auth_mech)

          if sasl_supported_mechs
            sasl_supported_mechs_size += 1                               # length of BSON type byte
            sasl_supported_mechs_size += 'saslSupportedMechs'.length + 1        # length of BSON key
            sasl_supported_mechs_size += 4                               # length of BSON string length
            sasl_supported_mechs_size += sasl_supported_mechs.length + 1 # length of BSON string
          end

          expect(app_metadata.ismaster_bytes.length).to eq(Mongo::Server::Monitor::Connection::ISMASTER_BYTES.length + sasl_supported_mechs_size + 26)
        end
      end
    end
  end
end