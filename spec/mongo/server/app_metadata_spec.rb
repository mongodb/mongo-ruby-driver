# frozen_string_literal: true

require 'spec_helper'

describe Mongo::Server::AppMetadata do
  let(:max_size) { described_class::Truncator::MAX_DOCUMENT_SIZE }

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
        expect(app_metadata.client_document[:application][:name]).to eq('app_metadata_test')
      end

      context 'when the app name exceeds the max length of 128' do
        let(:client) do
          authorized_client.with(app_name: "\u3042" * 43)
        end

        let(:cluster) do
          client.cluster
        end

        it 'raises an error' do
          expect { app_metadata.validated_document }
            .to raise_exception(Mongo::Error::InvalidApplicationName)
        end
      end
    end

    context 'when the cluster does not have an app name option set' do
      it 'does not set the app name' do
        expect(app_metadata.client_document[:application]).to be_nil
      end
    end

    context 'when the client document exceeds the max of 512 bytes' do
      shared_examples_for 'a truncated document' do
        it 'is too long before validation' do
          expect(app_metadata.client_document.to_bson.to_s.size).to be > max_size
        end

        it 'is acceptable after validation' do
          app_metadata.validated_document # force validation
          expect(app_metadata.client_document.to_bson.to_s.size).to be <= max_size
        end
      end

      context 'when the os.name length is too long' do
        before do
          allow(app_metadata).to receive(:name).and_return('x' * 500)
        end

        it_behaves_like 'a truncated document'
      end

      context 'when the os.architecture length is too long' do
        before do
          allow(app_metadata).to receive(:architecture).and_return('x' * 500)
        end

        it_behaves_like 'a truncated document'
      end

      context 'when the platform length is too long' do
        before do
          allow(app_metadata).to receive(:platform).and_return('x' * 500)
        end

        it_behaves_like 'a truncated document'
      end
    end

    context 'when run outside of a FaaS environment' do
      it 'excludes the :env key from the client document' do
        expect(app_metadata.client_document.key?(:env)).to be false
      end
    end

    context 'when run inside of a FaaS environment' do
      context 'when the environment is invalid' do
        # invalid, because it is missing the other required fields
        local_env('AWS_EXECUTION_ENV' => 'AWS_Lambda_ruby2.7')

        it 'excludes the :env key from the client document' do
          expect(app_metadata.client_document.key?(:env)).to be false
        end
      end

      context 'when the environment is valid' do
        # valid, because Azure requires only the one field
        local_env('FUNCTIONS_WORKER_RUNTIME' => 'ruby')

        it 'includes the :env key in the client document' do
          expect(app_metadata.client_document.key?(:env)).to be true
          expect(app_metadata.client_document[:env][:name]).to be == 'azure.func'
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

  describe '#validated_document' do
    it 'raises with too long app name' do
      app_name = 'app' * 500
      expect { described_class.new(app_name: app_name).validated_document }
        .to raise_error(Mongo::Error::InvalidApplicationName)
    end

    it 'does not raise with correct app name' do
      app_name = 'app'
      expect { described_class.new(app_name: app_name).validated_document }
        .not_to raise_error
    end
  end
end
