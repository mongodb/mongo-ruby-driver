# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::Error::MaxBSONSize do
  describe 'message' do
    context 'when constructor is given no arguments' do
      let(:error) do
        described_class.new
      end

      it 'is the predefined message' do
        error.message.should == 'The document exceeds maximum allowed BSON size'
      end
    end

    context 'when constructor is given an integer argument' do
      let(:error) do
        described_class.new(42)
      end

      it 'is the predefined message with the size added' do
        error.message.should == 'The document exceeds maximum allowed BSON size. The maximum allowed size is 42'
      end
    end

    context 'when constructor is given a string argument' do
      let(:error) do
        described_class.new('hello world')
      end

      it 'is the provided message' do
        error.message.should == 'hello world'
      end
    end
  end
end
