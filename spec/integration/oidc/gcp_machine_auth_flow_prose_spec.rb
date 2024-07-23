# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'OIDC Authentication Prose Tests' do
  require_oidc 'gcp'

  # Note that MONGODB_URI_SINGLE in the environment contains a valid GCP URI
  # with the correct ENVIRONMENT and TOKEN_RESOURCE auth mech properties. This
  # is populated by the drivers tools scripts that get the drivers/gcpoidc
  # secrets from the AWS secrets manager.
  describe 'GCP Machine Authentication Flow Prose Tests' do
    # No prose tests in the spec for GCP, testing the two cases
    context 'when the token resource is valid' do
      let(:client) do
        Mongo::Client.new(ENV.fetch('MONGODB_URI_SINGLE'), database: 'test')
      end

      let(:collection) do
        client['test']
      end

      after(:each) do
        client.close
      end

      it 'successfully authenticates' do
        expect(collection.find.to_a).to_not be_empty
      end
    end

    context 'when the token resource is invalid' do
      let(:client) do
        Mongo::Client.new(ENV.fetch('MONGODB_URI_SINGLE'),
          database: 'test',
          auth_mech_properties: {
            environment: 'gcp',
            token_resource: 'bad'
          }
        )
      end

      let(:collection) do
        client['test']
      end

      after(:each) do
        client.close
      end

      it 'fails authentication' do
        expect {
          collection.find.to_a
        }.to raise_error(Mongo::Auth::Unauthorized)
      end
    end
  end
end
