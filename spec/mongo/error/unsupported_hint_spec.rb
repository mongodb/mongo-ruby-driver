require 'spec_helper'

describe Mongo::Error::UnsupportedHint do
  describe '#new' do
    context 'with no arguments' do
      let(:error) { described_class.new }

      it 'creates an error with a default message' do
        expect(error.message).to eq(described_class::DEFAULT_MESSAGE)
      end
    end

    context 'with custom error message' do
      let(:message) { 'custom error message' }
      let(:error) { described_class.new(message) }

      it 'creates an error with the custom message' do
        expect(error.message).to eq(message)
      end
    end

    context 'with unacknowledged write and no message' do
      let(:error) { described_class.new(nil, unacknowledged_write: true) }
      
      it 'creates an error with the default unacknowledged write message' do
        expect(error.message).to eq(described_class::DEFAULT_UNACKNOWLEDGED_MESSAGE)
      end
    end

    context 'with unacknowledged write and a custom message' do
      let(:message) { 'custom error message' }
      let(:error) { described_class.new(message, unacknowledged_write: true) }
      
      it 'creates an error with the default unacknowledged write message' do
        expect(error.message).to eq(message)
      end
    end
  end
end
