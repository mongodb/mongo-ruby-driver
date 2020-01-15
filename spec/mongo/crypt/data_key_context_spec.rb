require 'mongo'
require 'support/lite_constraints'
require 'base64'

RSpec.configure do |config|
  config.extend(LiteConstraints)
end

describe Mongo::Crypt::DataKeyContext do
  require_libmongocrypt

  let(:mongocrypt) do
    Mongo::Crypt::Handle.new(
      { local: { key: Base64.encode64("ru\xfe\x00" * 24) } }
    )
  end

  let(:context) { described_class.new(mongocrypt) }

  describe '#initialize' do
    it 'does not raise an exception' do
      expect do
        context
      end.not_to raise_error
    end
  end

  # This is a simple spec just to test that this method works
  # There should be multiple specs testing the context's state
  #   depending on how it's initialized, etc.
  describe '#state' do
    it 'returns :ready' do
      expect(context.state).to eq(:ready)
    end
  end

  # This is a simple spec just to test the POC case of creating a data key
  # There should be specs testing each state, as well as integration tests
  #   to test that the state machine returns the correct result under various
  #   conditions
  describe '#run_state_machine' do
    it 'creates a data key' do
      expect(context.run_state_machine).to be_a_kind_of(Hash)
    end
  end
end
