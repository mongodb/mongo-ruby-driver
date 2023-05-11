# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::Error::Notable do
  let(:exception_cls) do
    # Since Notable is a module, we need a class that includes it for testing
    Mongo::Error
  end

  context 'when there are no notes' do
    let(:exception) do
      exception_cls.new('hello world')
    end

    describe '#message' do
      it 'is correct' do
        exception.message.should == 'hello world'
      end
    end

    describe '#to_s' do
      it 'is correct' do
        exception.to_s.should == 'hello world'
      end
    end

    describe '#inspect' do
      it 'is correct' do
        exception.inspect.should == '#<Mongo::Error: hello world>'
      end
    end
  end

  context 'when there are notes' do
    let(:exception) do
      exception_cls.new('hello world').tap do |exception|
        exception.add_note('brilliant')
        exception.add_note('weird')
      end
    end

    describe '#message' do
      it 'is correct' do
        exception.message.should == 'hello world (brilliant, weird)'
      end
    end

    describe '#to_s' do
      it 'is correct' do
        exception.to_s.should == 'hello world (brilliant, weird)'
      end
    end

    describe '#inspect' do
      it 'is correct' do
        exception.inspect.should == '#<Mongo::Error: hello world (brilliant, weird)>'
      end
    end
  end
end
