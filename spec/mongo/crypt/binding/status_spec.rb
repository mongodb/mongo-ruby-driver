require 'mongo'
require 'support/lite_constraints'

RSpec.configure do |config|
  config.extend(LiteConstraints)
end

describe 'Mongo::Crypt::Binding' do
  describe 'mongocrypt_status_t binding' do
    let(:status) { Mongo::Crypt::Binding.mongocrypt_status_new }

    after do
      Mongo::Crypt::Binding.mongocrypt_status_destroy(status)
    end

    describe '#mongocrypt_status_new' do
      it 'returns a pointer' do
        expect(status).to be_a_kind_of(FFI::Pointer)
      end
    end

    describe '#mongo_status_set' do
      let(:string) { "Operation successful" }

      it 'doesn\'t raise an error' do
        expect do
          Mongo::Crypt::Binding.mongocrypt_status_set(
            status,
            :ok,
            200,
            string,
            string.length + 1
          )
        end.not_to raise_error
      end
    end
  end
end
