# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::Crypt::Binary do
  require_libmongocrypt

  let(:data) { 'I love Ruby' }
  let(:binary) { described_class.from_data(data) }

  describe '#initialize' do
    context 'with nil data' do
      let(:binary) { described_class.new }

      it 'creates a new Mongo::Crypt::Binary object' do
        expect do
          binary
        end.not_to raise_error
      end
    end

    context 'with valid data' do
      let(:binary) { described_class.new(data: data) }

      it 'creates a new Mongo::Crypt::Binary object' do
        expect do
          binary
        end.not_to raise_error
      end
    end

    context 'with pointer' do
      let(:pointer) { Mongo::Crypt::Binding.mongocrypt_binary_new }
      let(:binary) { described_class.new(pointer: pointer) }

      after do
        Mongo::Crypt::Binding.mongocrypt_binary_destroy(pointer)
      end

      it 'creates a new Mongo::Crypt::Binary object from pointer' do
        expect do
          binary
        end.not_to raise_error

        expect(binary.ref).to eq(pointer)
      end
    end
  end

  describe '#self.from_data' do
    let(:binary) { described_class.from_data(data) }

    it 'creates a new Mongo::Crypt::Binary object' do
      expect do
        binary
      end.not_to raise_error
    end
  end

  describe '#self.from_pointer' do
    let(:pointer) { Mongo::Crypt::Binding.mongocrypt_binary_new }
    let(:binary) { described_class.from_pointer(pointer) }

    after do
      Mongo::Crypt::Binding.mongocrypt_binary_destroy(pointer)
    end

    it 'creates a new Mongo::Crypt::Binary object from pointer' do
      expect do
        binary
      end.not_to raise_error

      expect(binary.ref).to eq(pointer)
    end
  end

  describe '#to_s' do
    it 'returns the original string' do
      expect(binary.to_s).to eq(data)
    end
  end

  describe '#write' do
    # Binary must have enough space pre-allocated
    let(:binary) { described_class.from_data("\00" * data.length) }

    it 'writes data to the binary object' do
      expect(binary.write(data)).to be true
      expect(binary.to_s).to eq(data)
    end

    context 'with no space allocated' do
      let(:binary) { described_class.new }

      it 'returns false' do
        expect do
          binary.write(data)
        end.to raise_error(ArgumentError, /Cannot write #{data.length} bytes of data to a Binary object that was initialized with 0 bytes/)
      end
    end

    context 'without enough space allocated' do
      let(:binary) { described_class.from_data("\00" * (data.length - 1)) }

      it 'returns false' do
        expect do
          binary.write(data)
        end.to raise_error(ArgumentError, /Cannot write #{data.length} bytes of data to a Binary object that was initialized with #{data.length - 1} bytes/)
      end
    end
  end
end
