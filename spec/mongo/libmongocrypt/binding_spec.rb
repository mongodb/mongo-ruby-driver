require 'mongo'
require 'support/lite_constraints'

RSpec.configure do |config|
  config.extend(LiteConstraints)
end

describe 'Mongo::Libmongocrypt::Binding' do
  require_libmongocrypt

  describe '#mongocrypt_version' do
    let(:version) { Mongo::Libmongocrypt::Binding.mongocrypt_version(nil) }

    it 'is a string' do
      expect(version).to be_a_kind_of(String)
    end

    it 'is in the x.y.z-tag format' do
      expect(version).to match(/\A(\d+.){2}(\d+)?(-[A-Za-z\d]+)?\z/)
    end
  end

  describe 'binary_t bindings' do
    let(:bytes) { [104, 101, 108, 108, 111] }

    let(:bytes_pointer) do
      # FFI::MemoryPointer automatically frees memory when it goes out of scope
      p = FFI::MemoryPointer.new(bytes.size)
      p.write_array_of_type(FFI::TYPE_UINT8, :put_uint8, bytes)
    end

    after do
      Mongo::Libmongocrypt::Binding.mongocrypt_binary_destroy(binary)
    end

    describe '#mongocrypt_binary_new' do
      let(:binary) { Mongo::Libmongocrypt::Binding.mongocrypt_binary_new }

      it 'returns a pointer' do
        expect(binary).to be_a_kind_of(FFI::Pointer)
      end
    end

    describe '#mongocrypt_binary_new_from_data' do
      let(:binary) { Mongo::Libmongocrypt::Binding.mongocrypt_binary_new_from_data(bytes_pointer, bytes.length) }

      it 'returns a pointer' do
        expect(binary).to be_a_kind_of(FFI::Pointer)
      end
    end

    describe '#mongocrypt_binary_data' do
      let(:binary) { Mongo::Libmongocrypt::Binding.mongocrypt_binary_new_from_data(bytes_pointer, bytes.length) }

      it 'returns the pointer to the data' do
        expect(Mongo::Libmongocrypt::Binding.mongocrypt_binary_data(binary)).to eq(bytes_pointer)
      end
    end

    describe '#mongocrypt_binary_len' do
      let(:binary) { Mongo::Libmongocrypt::Binding.mongocrypt_binary_new_from_data(bytes_pointer, bytes.length) }

      it 'returns the length of the data' do
        expect(Mongo::Libmongocrypt::Binding.mongocrypt_binary_len(binary)).to eq(bytes.length)
      end
    end
  end
end
