require 'mongo'
require 'support/lite_constraints'

RSpec.configure do |config|
  config.extend(LiteConstraints)
end

describe 'Mongo::Crypt::Binding' do
  require_libmongocrypt

  describe '#mongocrypt_version' do
    let(:version) { Mongo::Crypt::Binding.mongocrypt_version(nil) }

    it 'is a string' do
      expect(version).to be_a_kind_of(String)
    end

    it 'is in the x.y.z-tag format' do
      expect(version).to match(/\A(\d+.){2}(\d+)?(-[A-Za-z\+\d]+)?\z/)
    end
  end
end
