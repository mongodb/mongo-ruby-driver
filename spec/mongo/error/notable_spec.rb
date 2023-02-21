# frozen_string_literal: true
# encoding: utf-8

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
        expect(exception.message).to eq('hello world')
      end
    end

    describe '#to_s' do
      it 'is correct' do
        expect(exception.to_s).to eq('hello world')
      end
    end

    describe '#inspect' do
      it 'is correct' do
        expect(exception.inspect).to eq('#<Mongo::Error: hello world>')
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
        expect(exception.message).to eq('hello world (brilliant, weird)')
      end
    end

    describe '#to_s' do
      it 'is correct' do
        expect(exception.to_s).to eq('hello world (brilliant, weird)')
      end
    end

    describe '#inspect' do
      it 'is correct' do
        expect(exception.inspect).to eq('#<Mongo::Error: hello world (brilliant, weird)>')
      end
    end
  end
end
