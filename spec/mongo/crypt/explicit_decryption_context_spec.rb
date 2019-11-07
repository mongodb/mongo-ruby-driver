require 'mongo'
require 'support/lite_constraints'

RSpec.configure do |config|
  config.extend(LiteConstraints)
end

describe Mongo::Crypt::ExplicitDecryptionContext do
  require_libmongocrypt

  let(:context) { described_class.new(mongocrypt, io, value) }

  let(:mongocrypt) { Mongo::Crypt::Binding.mongocrypt_new }
  let(:io) { double("Mongo::ClientEncryption::IO") }
  let(:value) { BSON::Binary.new("o\x00\x00\x00\x05v\x00b\x00\x00\x00\x06\x01\xDF2~\x89\xD2+N}\x84;i(\xE5\xF4\xBF \x024\xE5\xD2\n\x9E\x97\x9F\xAF\x9D\xC7\xC9\x1A\a\x87z\xAE_;r\xAC\xA9\xF6n\x1D\x0F\xB5\xB1#O\xB7\xCA\xEE$/\xF1\xFA\b\xA7\xEC\xDB\xB6\xD4\xED\xEAMw3+\xBBv\x18\x97\xF9\x99\xD5\x13@\x80y\n{\x19R\xD3\xF0\xA1C\x05\xF7)\x93\x9Bh\x8AA.\xBB\xD3&\xEA\x00") }

  before do
    Mongo::Crypt::Binding.mongocrypt_init(mongocrypt)
  end

  after do
    Mongo::Crypt::Binding.mongocrypt_destroy(mongocrypt)
  end

  describe '#initialize' do
    it 'initializes context' do
      expect do
        context
      end.not_to raise_error
    end
  end
end
