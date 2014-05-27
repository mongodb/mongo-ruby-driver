require 'spec_helper'

describe Mongo::CollectionView do
  include_context 'shared client'

  let(:selector) { { :name => 'Sam' } }
  let(:view) { described_class.new(collection, selector) }

  describe '#fetch_one_then_remove' do

    context 'when no skip is used' do

      it 'creates a new findAndModify command' do
        #expect(Mongo::Operation::Command).to receive(:new)
      end

      it 'executes a findAndModify command' do
        #expect_any_instance_of(Mongo::Operation::Command).to receive(:execute)
      end

      it 'returns a single document' do
        #expect(view.fetch_one_then_remove).to be_a(Hash)
      end
    end

    context 'when skip is specified' do

      it 'raises an exception' do
        expect{ view.skip(10).fetch_one_then_remove }.to raise_error
      end
    end
  end

  describe '#fetch_one_then_replace' do

    context 'when no skip is used' do

      it 'creates a new findAndModify command' do
        #expect(Mongo::Operation::Command).to receive(:new)
      end

      it 'executes a findAndModify command' do
        #expect_any_instance_of(Mongo::Operation::Command).to receive(:execute)
      end

      it 'returns a single document' do
        #expect(view.fetch_one_then_replace).to be_a(Hash)
      end
    end

    context 'when skip is specified' do

      it 'raises an exception' do
        expect{ view.skip(10).fetch_one_then_replace }.to raise_error
      end
    end
  end

  describe '#replace_one_then_fetch' do

    context 'when no skip is used' do

      it 'creates a new findAndModify command' do
        #expect(Mongo::Operation::Command).to receive(:new)
      end

      it 'executes a findAndModify command' do
        #expect_any_instance_of(Mongo::Operation::Command).to receive(:execute)
      end

      it 'returns a single document' do
        #expect(view.replace_one_then_fetch).to be_a(Hash)
      end
    end

    context 'when skip is specified' do

      it 'raises an exception' do
        expect{ view.skip(10).replace_one_then_fetch }.to raise_error
      end
    end
  end

  describe '#update_one_then_fetch' do

    context 'when no skip is used' do

      it 'creates a new findAndModify command' do
        #expect(Mongo::Operation::Command).to receive(:new)
      end

      it 'executes a findAndModify command' do
        #expect_any_instance_of(Mongo::Operation::Command).to receive(:execute)
      end

      it 'returns a single document' do
        #expect(view.update_one_then_fetch).to be_a(Hash)
      end
    end

    context 'when skip is specified' do

      it 'raises an exception' do
        expect{ view.skip(10).update_one_then_fetch }.to raise_error
      end
    end
  end
end
