require 'lite_spec_helper'

describe Mongo::Libmongocrypt::Binding do
  describe '#mongocrypt_version' do
    let(:version) { described_class.mongocrypt_version(nil) }

    it 'is a string' do
      expect(version).to be_a_kind_of(String)
    end

    it 'is in the x.y.z-tag format' do
      expect(version).to match(/^(\d+.){2}(\d+)?(-[A-Za-z\d]+)?$/)
    end
  end
end
