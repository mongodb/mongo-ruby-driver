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
      p = FFI::MemoryPointer.new(bytes.size)
      p.write_array_of_type(FFI::TYPE_UINT8, :put_uint8, bytes)
    end

    describe '#mongocrypt_binary_new' do
      it 'returns a pointer' do
        expect(Mongo::Libmongocrypt::Binding.mongocrypt_binary_new).to be_a_kind_of(FFI::Pointer)
      end
    end

    describe '#mongocrypt_binary_new_from_data' do
      it 'returns a pointer' do
        expect(Mongo::Libmongocrypt::Binding.mongocrypt_binary_new_from_data(bytes_pointer, bytes.length)).to be_a_kind_of(FFI::Pointer)
      end
    end

    describe '#mongocrypt_binary_data' do
      it 'returns the pointer to the data' do
        binary_p = Mongo::Libmongocrypt::Binding.mongocrypt_binary_new_from_data(bytes_pointer, bytes.length)
        expect(Mongo::Libmongocrypt::Binding.mongocrypt_binary_data(binary_p)).to eq(bytes_pointer)
      end
    end

    describe '#mongocrypt_binary_len' do
      it 'returns the length of the data' do
        binary_p = Mongo::Libmongocrypt::Binding.mongocrypt_binary_new_from_data(bytes_pointer, bytes.length)
        expect(Mongo::Libmongocrypt::Binding.mongocrypt_binary_len(binary_p)).to eq(bytes.length)
      end
    end

    describe '#mongocrypt_binary_destroy' do
      it 'destroys the reference to the binary object' do
        binary_p = Mongo::Libmongocrypt::Binding.mongocrypt_binary_new_from_data(bytes_pointer, bytes.length)

        Mongo::Libmongocrypt::Binding.mongocrypt_binary_destroy(binary_p)
      end
    end
  end
end
