require 'spec_helper'

describe Mongo::TLSContextHooks do
  before do
    Mongo::TLSContextHooks.clear_hooks
  end

  describe '#hooks' do
    it 'returns an array' do
      expect(Mongo::TLSContextHooks.hooks).to eq([])
    end
  end

  describe '#hooks=' do
    context 'when argument is not an array' do
      it 'raises an ArgumentError' do
        expect do
          Mongo::TLSContextHooks.hooks = "Hello"
        end.to raise_error(ArgumentError, /TLS context hooks must be an array of Procs/)
      end
    end

    context 'when argument is an array not containing procs' do
      it 'raises an ArgumentError' do
        expect do
          Mongo::TLSContextHooks.hooks = [1, 2, 3]
        end.to raise_error(ArgumentError, /TLS context hooks must be an array of Procs/)
      end
    end

    it 'saves the provided hooks' do
      Mongo::TLSContextHooks.hooks = [ Proc.new { |x| x ** 2 } ]
      expect(Mongo::TLSContextHooks.hooks.length).to eq(1)
      expect(Mongo::TLSContextHooks.hooks.first).to be_a(Proc)
    end
  end
end
