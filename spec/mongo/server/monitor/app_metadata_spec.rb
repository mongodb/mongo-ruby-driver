require 'lite_spec_helper'

describe Mongo::Server::Monitor::AppMetadata do

  describe '#document' do
    context 'when user is given and auth_mech is not given' do
      let(:app_metadata) do
        described_class.new(user: 'foo')
      end

      it 'does not include saslSupportedMechs' do
        expect(app_metadata.send(:document)).not_to have_key(:saslSupportedMechs)
      end
    end
  end
end
