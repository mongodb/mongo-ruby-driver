# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Auth::Oidc::MachineWorkflow::CallbackFactory do
  describe '.get_callback' do
    context 'when an OIDC_CALLBACK auth mech property is provided' do
      class OidcCallback
        def execute(params: {})
          { access_token: 'test' }
        end
      end

      let(:callback) do
        described_class.get_callback(auth_mech_properties: { oidc_callback: OidcCallback.new })
      end

      it 'returns the user provided callback' do
        expect(callback).to be_a OidcCallback
      end
    end

    context 'when an environment auth mech property is provided' do
      context 'when the value is azure' do
        let(:callback) do
          described_class.get_callback(auth_mech_properties: { environment: 'azure', token_resource: 'resource' })
        end

        it 'returns the azure callback' do
          expect(callback).to be_a Mongo::Auth::Oidc::MachineWorkflow::AzureCallback
        end
      end

      context 'when the valie is gcp' do
        let(:callback) do
          described_class.get_callback(auth_mech_properties: { environment: 'gcp', token_resource: 'client_id' })
        end

        it 'returns the gcp callback' do
          expect(callback).to be_a Mongo::Auth::Oidc::MachineWorkflow::GcpCallback
        end
      end

      context 'when the value is test' do
        let(:callback) do
          described_class.get_callback(auth_mech_properties: { environment: 'test' })
        end

        it 'returns the test callback' do
          expect(callback).to be_a Mongo::Auth::Oidc::MachineWorkflow::TestCallback
        end
      end

      context 'when the value is unknown' do
        it 'raises an oidc error' do
          expect {
            described_class.get_callback(auth_mech_properties: { environment: 'nothing' })
          }.to raise_error(Mongo::Error::OidcError)
        end
      end
    end
  end
end
