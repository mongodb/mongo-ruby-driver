# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'OIDC Authentication Prose Tests' do
  require_oidc 'azure'

  # Note that MONGODB_URI_SINGLE in the environment contains a valid Azure URI
  # with the correct ENVIRONMENT and TOKEN_RESOURCE auth mech properties. This
  # is populated by the drivers tools scripts that get the drivers/azureoidc
  # secrets from the AWS secrets manager.
  describe 'Azure Machine Authentication Flow Prose Tests' do
    # 5.1 Azure With No Username
    context 'when no username is provided' do
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

    # 5.2 Azure With Bad Username
    context 'when a bad username is provided' do
      let(:client) do
        Mongo::Client.new(ENV.fetch('MONGODB_URI_SINGLE'), database: 'test', user: 'bad')
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
        }.to raise_error(Mongo::Error::OidcError)
      end
    end

    # No prose test in spec for this but is a valid test case.
    context 'when a valid username is provided' do
      let(:client) do
        Mongo::Client.new(ENV.fetch('MONGODB_URI_SINGLE'),
          database: 'test',
          user: ENV.fetch('AZUREOIDC_USERNAME')
        )
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
  end
end
