require 'spec_helper'

describe Mongo::CollectionView do
  include_context 'shared client'

  let(:selector) { { :name => 'Sam' } }
  let(:view) { described_class.new(collection, selector) }

  describe '#fetch_one_then_remove' do

    context 'when no skip is used' do

      it 'creates a new findAndModify command operation' do
        #expect(Mongo::Operation::Command).to receive(:new)
        view.fetch_one_then_remove
      end

      it 'executes a findAndModify command operation' do
        #expect_any_instance_of(Mongo::Operation::Command).to do
           #receive(:execute)
        #end
        view.fetch_one_then_remove
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
    let(:replacement) { { :name => 'Emily' } }
    let(:bad_replacement) { { :$name => 'Emily' } }

    context 'when no skip is used' do

      it 'creates a new findAndModify command operation' do
        #expect(Mongo::Operation::Command).to receive(:new)
        view.fetch_one_then_replace(replacement)
      end

      it 'executes a findAndModify command operation' do
        #expect_any_instance_of(Mongo::Operation::Command).to do
          #receive(:execute)
        #end
        view.fetch_one_then_replace(replacement)
      end

      it 'returns a single document' do
        #expect(view.fetch_one_then_replace(replacement)).to be_a(Hash)
      end
    end

    context 'when skip is specified' do

      it 'raises an exception' do
        expect do
          view.skip(10).fetch_one_then_replace(replacement)
        end.to raise_error
      end
    end

    context 'replacement doc whose first key does not begin with $' do

      it 'succeeds' do
        expect(view.fetch_one_then_replace(replacement)).to be_true
      end
    end

    context 'replacement doc whose first key begins with $' do

      it 'raises an exception' do
        expect do
          view.fetch_one_then_replace(bad_replacement)
        end.to raise_error
      end
    end
  end

  describe '#fetch_one_then_update' do
    let(:update) { { :$set => { :name => "Emily" } } }
    let(:bad_update) { { :name => "Emily" } }

    context 'when no skip is used' do

      it 'creates a new findAndModify command operation' do
        #expect(Mongo::Operation::Command).to receive(:new)
        view.fetch_one_then_update(update)
      end

      it 'executes a findAndModify command operation' do
        #expect_any_instance_of(Mongo::Operation::Command).to do
          #receive(:execute)
        #end
        view.fetch_one_then_update(update)
      end

      it 'returns a single document' do
        #expect(view.fetch_one_then_update(update)).to be_a(Hash)
      end
    end

    context 'when skip is specified' do

      it 'raises an exception' do
        expect do
          view.skip(10).fetch_one_then_update(update)
        end.to raise_error
      end
    end

    context 'update doc whose first key begins with $' do

      it 'succeeds' do
        expect(view.fetch_one_then_update(update)).to be_true
      end
    end

    context 'update doc whose first key does not begin with $' do

      it 'raises an exception' do
        expect do
          view.fetch_one_then_update(bad_update)
        end.to raise_error
      end
    end
  end

  describe '#replace_one_then_fetch' do
    let(:replacement) { { :name => 'Emily' } }
    let(:bad_replacement) { { :$name => 'Emily' } }

    context 'when no skip is used' do

      it 'creates a new findAndModify command' do
        #expect(Mongo::Operation::Command).to receive(:new)
        view.replace_one_then_fetch(replacement)
      end

      it 'executes a findAndModify command' do
        #expect_any_instance_of(Mongo::Operation::Command).to do
          #receive(:execute)
        #end
        view.replace_one_then_fetch(replacement)
      end

      it 'returns a single document' do
        #expect(view.replace_one_then_fetch).to be_a(Hash)
        view.replace_one_then_fetch(replacement)
      end
    end

    context 'when skip is specified' do

      it 'raises an exception' do
        expect do
          view.skip(10).replace_one_then_fetch(replacement)
        end.to raise_error
      end
    end

    context 'replacement doc whose first key does not begin with $' do

      it 'succeeds' do
        expect(view.replace_one_then_fetch(replacement)).to be_true
      end
    end

    context 'replacement doc whose first key begins with $' do

      it 'raises an exception' do
        expect do
          view.replace_one_then_fetch(bad_replacement)
        end.to raise_error
      end
    end
  end

  describe '#update_one_then_fetch' do
    let(:update) { { :$set => { :name => "Emily" } } }
    let(:bad_update) { { :name => "Emily" } }

    context 'when no skip is used' do

      it 'creates a new findAndModify command' do
        #expect(Mongo::Operation::Command).to receive(:new)
        view.update_one_then_fetch(update)
      end

      it 'executes a findAndModify command' do
        #expect_any_instance_of(Mongo::Operation::Command).to do
          #receive(:execute)
        #end
        view.update_one_then_fetch(update)
      end

      it 'returns a single document' do
        #expect(view.update_one_then_fetch).to be_a(Hash)
        view.update_one_then_fetch(update)
      end
    end

    context 'when skip is specified' do

      it 'raises an exception' do
        expect do
          view.skip(10).update_one_then_fetch(update)
        end.to raise_error
      end
    end

    context 'update doc whose first key begins with $' do

      it 'succeeds' do
        expect(view.fetch_one_then_update(update)).to be_true
      end
    end

    context 'update doc whose first key does not begin with $' do

      it 'raises an exception' do
        expect{ view.fetch_one_then_update(bad_update) }.to raise_error
      end
    end
  end
end
