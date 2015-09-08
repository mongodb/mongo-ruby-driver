require 'spec_helper'

describe Mongo::Options::Redacted do

  let(:options) do
    described_class.new(original_opts)
  end

  describe '#to_s' do

    context 'when the hash contains a sensitive key' do

      let(:original_opts) do
        { password: '123' }
      end

      it 'replaces the value with the redacted string' do
        expect(options.to_s).not_to match(/123/)
      end

      it 'replaces the value with the redacted string' do
        expect(options.to_s).to match(Mongo::Options::Redacted::STRING_REPLACEMENT)
      end
    end

    context 'when the hash does not contain a sensitive key' do

      it 'prints all the values' do

      end
    end
  end

  describe '#inspect' do

    context 'when the hash contains a sensitive key' do

      let(:original_opts) do
        { password: '123' }
      end

      it 'replaces the value with the redacted string' do
        expect(options.inspect).not_to match(/123/)
      end

      it 'replaces the value with the redacted string' do
        expect(options.inspect).to match(Mongo::Options::Redacted::STRING_REPLACEMENT)
      end
    end

    context 'when the hash does not contain a sensitive key' do

      let(:original_opts) do
        { name: '123' }
      end

      it 'does not replace the value with the redacted string' do
        expect(options.inspect).to match(/123/)
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

        it 'returns true when ' do
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

        it 'returns true when ' do
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

        it 'returns true when ' do
          expect(options.has_key?('name')).to be(false)
        end
      end

      context 'when method argument is a Symbol' do

        it 'returns true' do
          expect(options.has_key?(:name)).to be(false)
        end
      end
    end
  end
end