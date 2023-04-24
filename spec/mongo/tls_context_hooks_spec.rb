# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo do
  before do
    Mongo.tls_context_hooks.clear
  end

  describe '#tls_context_hooks' do
    it 'returns an array' do
      expect(Mongo.tls_context_hooks).to eq([])
    end
  end

  describe '#tls_context_hooks=' do
    context 'when argument is not an array' do
      it 'raises an ArgumentError' do
        expect do
          Mongo.tls_context_hooks = "Hello"
        end.to raise_error(ArgumentError, /TLS context hooks must be an array of Procs/)
      end
    end

    context 'when argument is an array not containing procs' do
      it 'raises an ArgumentError' do
        expect do
          Mongo.tls_context_hooks = [1, 2, 3]
        end.to raise_error(ArgumentError, /TLS context hooks must be an array of Procs/)
      end
    end

    it 'saves the provided hooks' do
      Mongo.tls_context_hooks = [ Proc.new { |x| x ** 2 } ]
      expect(Mongo.tls_context_hooks.length).to eq(1)
      expect(Mongo.tls_context_hooks.first).to be_a(Proc)
    end
  end
end
