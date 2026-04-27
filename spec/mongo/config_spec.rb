# frozen_string_literal: true

require 'spec_helper'

describe Mongo::Config do
  shared_examples 'a config option' do
    before do
      Mongo::Config.reset
    end

    context 'when the value is false' do
      before do
        Mongo.send("#{option}=", false)
      end

      it 'is set to false' do
        expect(Mongo.send(option)).to be(false)
      end
    end

    context 'when the value is true' do
      before do
        Mongo.send("#{option}=", true)
      end

      it 'is set to true' do
        expect(Mongo.send(option)).to be(true)
      end
    end

    context 'when it is not set in the config' do
      it 'is set to its default' do
        expect(Mongo.send(option)).to be(default)
      end
    end
  end

  context 'when setting the validate_update_replace option in the config' do
    let(:option) { :validate_update_replace }
    let(:default) { false }

    it_behaves_like 'a config option'
  end

  describe '#options=' do
    context 'when an option' do
      before do
        described_class.options = { validate_update_replace: true }
      end

      it 'assigns the option correctly' do
        expect(described_class.validate_update_replace).to be true
      end
    end

    context 'when provided a non-existent option' do
      it 'raises an error' do
        expect do
          described_class.options = { bad_option: true }
        end.to raise_error(Mongo::Error::InvalidConfigOption)
      end
    end
  end

  describe '.include_server_address_in_errors' do
    it 'defaults to false' do
      expect(Mongo::Config.include_server_address_in_errors).to be false
    end

    it 'is accessible as Mongo.include_server_address_in_errors' do
      expect(Mongo.include_server_address_in_errors).to be false
    end

    it 'can be set via Mongo=' do
      original = Mongo.include_server_address_in_errors
      Mongo.include_server_address_in_errors = true
      expect(Mongo.include_server_address_in_errors).to be true
    ensure
      Mongo.include_server_address_in_errors = original
    end
  end
end
