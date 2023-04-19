# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Options::Redacted do

  let(:options) do
    described_class.new(original_opts)
  end

  describe '#to_s' do

    context 'when the hash contains a sensitive key' do

      let(:original_opts) do
        { password: 'sensitive_data' }
      end

      it 'replaces the value with the redacted string' do
        expect(options.to_s).not_to match(original_opts[:password])
      end

      it 'replaces the value with the redacted string' do
        expect(options.to_s).to match(Mongo::Options::Redacted::STRING_REPLACEMENT)
      end
    end

    context 'when the hash does not contain a sensitive key' do

      let(:original_opts) do
        { user: 'emily' }
      end

      it 'prints all the values' do
        expect(options.to_s).to match(original_opts[:user])
      end
    end
  end

  describe '#inspect' do

    context 'when the hash contains a sensitive key' do

      let(:original_opts) do
        { password: 'sensitive_data' }
      end

      it 'replaces the value with the redacted string' do
        expect(options.inspect).not_to match(original_opts[:password])
      end

      it 'replaces the value with the redacted string' do
        expect(options.inspect).to match(Mongo::Options::Redacted::STRING_REPLACEMENT)
      end
    end

    context 'when the hash does not contain a sensitive key' do

      let(:original_opts) do
        { name: 'some_name' }
      end

      it 'does not replace the value with the redacted string' do
        expect(options.inspect).to match(original_opts[:name])
      end

      it 'does not replace the value with the redacted string' do
        expect(options.inspect).not_to match(Mongo::Options::Redacted::STRING_REPLACEMENT)
      end
    end
  end

  describe '#has_key?' do

    context 'when the original key is a String' do

      let(:original_opts) do
        { 'name' => 'Emily' }
      end

      context 'when the method argument is a String' do

        it 'returns true' do
          expect(options.has_key?('name')).to be(true)
        end
      end

      context 'when method argument is a Symbol' do

        it 'returns true' do
          expect(options.has_key?(:name)).to be(true)
        end
      end
    end

    context 'when the original key is a Symbol' do

      let(:original_opts) do
        { name: 'Emily' }
      end

      context 'when the method argument is a String' do

        it 'returns true' do
          expect(options.has_key?('name')).to be(true)
        end
      end

      context 'when method argument is a Symbol' do

        it 'returns true' do
          expect(options.has_key?(:name)).to be(true)
        end
      end
    end

    context 'when the hash does not contain the key' do

      let(:original_opts) do
        { other: 'Emily' }
      end

      context 'when the method argument is a String' do

        it 'returns false' do
          expect(options.has_key?('name')).to be(false)
        end
      end

      context 'when method argument is a Symbol' do

        it 'returns false' do
          expect(options.has_key?(:name)).to be(false)
        end
      end
    end
  end

  describe '#reject' do

    let(:options) do
      described_class.new(a: 1, b: 2, c: 3)
    end

    context 'when no block is provided' do

      it 'returns an enumerable' do
        expect(options.reject).to be_a(Enumerator)
      end
    end

    context 'when a block is provided' do

      context 'when the block evaluates to true for some pairs' do

        let(:result) do
          options.reject { |k,v| k == 'a' }
        end

        it 'returns an object consisting of only the remaining pairs' do
          expect(result).to eq(described_class.new(b: 2, c: 3))
        end

        it 'returns a new object' do
          expect(result).not_to be(options)
        end
      end

      context 'when the block does not evaluate to true for any pairs' do

        let(:result) do
          options.reject { |k,v| k == 'd' }
        end

        it 'returns an object with all pairs intact' do
          expect(result).to eq(described_class.new(a: 1, b: 2, c: 3))
        end

        it 'returns a new object' do
          expect(result).not_to be(options)
        end
      end
    end
  end

  describe '#reject!' do

    let(:options) do
      described_class.new(a: 1, b: 2, c: 3)
    end

    context 'when no block is provided' do

      it 'returns an enumerable' do
        expect(options.reject).to be_a(Enumerator)
      end
    end

    context 'when a block is provided' do

      context 'when the block evaluates to true for some pairs' do

        let(:result) do
          options.reject! { |k,v| k == 'a' }
        end

        it 'returns an object consisting of only the remaining pairs' do
          expect(result).to eq(described_class.new(b: 2, c: 3))
        end

        it 'returns the same object' do
          expect(result).to be(options)
        end
      end

      context 'when the block does not evaluate to true for any pairs' do

        let(:result) do
          options.reject! { |k,v| k == 'd' }
        end

        it 'returns nil' do
          expect(result).to be(nil)
        end
      end
    end
  end

  describe '#select' do

    let(:options) do
      described_class.new(a: 1, b: 2, c: 3)
    end

    context 'when no block is provided' do

      it 'returns an enumerable' do
        expect(options.reject).to be_a(Enumerator)
      end
    end

    context 'when a block is provided' do

      context 'when the block evaluates to true for some pairs' do

        let(:result) do
          options.select { |k,v| k == 'a' }
        end

        it 'returns an object consisting of those pairs' do
          expect(result).to eq(described_class.new(a: 1))
        end

        it 'returns a new object' do
          expect(result).not_to be(options)
        end
      end

      context 'when the block does not evaluate to true for any pairs' do

        let(:result) do
          options.select { |k,v| k == 'd' }
        end

        it 'returns an object with no pairs' do
          expect(result).to eq(described_class.new)
        end

        it 'returns a new object' do
          expect(result).not_to be(options)
        end
      end

      context 'when the object is unchanged' do

        let(:options) do
          described_class.new(a: 1, b: 2, c: 3)
        end

        let(:result) do
          options.select { |k,v| ['a', 'b', 'c'].include?(k) }
        end

        it 'returns a new object' do
          expect(result).to eq(described_class.new(a: 1, b: 2, c: 3))
        end
      end
    end
  end

  describe '#select!' do

    let(:options) do
      described_class.new(a: 1, b: 2, c: 3)
    end

    context 'when no block is provided' do

      it 'returns an enumerable' do
        expect(options.reject).to be_a(Enumerator)
      end
    end

    context 'when a block is provided' do

      context 'when the block evaluates to true for some pairs' do

        let(:result) do
          options.select! { |k,v| k == 'a' }
        end

        it 'returns an object consisting of those pairs' do
          expect(result).to eq(described_class.new(a: 1))
        end

        it 'returns the same object' do
          expect(result).to be(options)
        end
      end

      context 'when the block does not evaluate to true for any pairs' do

        let(:result) do
          options.select! { |k,v| k == 'd' }
        end

        it 'returns an object with no pairs' do
          expect(result).to eq(described_class.new)
        end

        it 'returns the same object' do
          expect(result).to be(options)
        end
      end

      context 'when the object is unchanged' do

        let(:options) do
          described_class.new(a: 1, b: 2, c: 3)
        end

        let(:result) do
          options.select! { |k,v| ['a', 'b', 'c'].include?(k) }
        end

        it 'returns nil' do
          expect(result).to be(nil)
        end
      end
    end
  end
end
