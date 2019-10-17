require 'lite_spec_helper'

describe 'Mongo::Libmongocrypt::Binding' do
  before do
    unless ENV['LIBMONGOCRYPT_PATH']
      skip 'Test requires path to libmongocrypt to be specified in LIBMONGOCRYPT_PATH env variable'
    end
  end

  describe '#mongocrypt_version' do
    let(:version) { Mongo::Libmongocrypt::Binding.mongocrypt_version(nil) }

    it 'is a string' do
      expect(version).to be_a_kind_of(String)
    end

    it 'is in the x.y.z-tag format' do
      expect(version).to match(/^(\d+.){2}(\d+)?(-[A-Za-z\d]+)?$/)
    end
  end
end
