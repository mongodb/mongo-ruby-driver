# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::Server::Monitor::AppMetadata do

  describe '#document' do
    let(:document) do
      app_metadata.send(:document)
    end

    context 'when user is given and auth_mech is not given' do
      let(:app_metadata) do
        described_class.new(user: 'foo')
      end

      it 'does not include saslSupportedMechs' do
        expect(document).not_to have_key(:saslSupportedMechs)
      end
    end

    it_behaves_like 'app metadata document'
  end
end
