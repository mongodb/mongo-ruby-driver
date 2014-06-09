require 'spec_helper'

describe Mongo::CollectionView do
  include_context 'shared client'

  let(:selector) { { :name => 'Emily' } }
  let(:view) { described_class.new(collection, selector) }

  describe '#remove' do

    context 'when sort has been specified' do

      it 'raises an exception' do
        expect{ view.sort({ :a => 1 }).remove }.to raise_error
      end
    end

    context 'when skip has been specified' do

      it 'raises an exception' do
        expect{ view.skip(2).remove }.to raise_error
      end
    end

    context 'when limit other than 1 has been specified' do

      it 'raises an exception' do
        expect{ view.limit(2).remove }.to raise_error
      end
    end

    context 'delete operation' do

      it 'creates a delete operation with the correct spec' do
        # @todo check spec
        #expect(Mongo::Operation::Delete).to receive(:new)
        view.remove
      end

      it 'executes a delete operation' do
        #expect_any_instance_of(Mongo::Operation::Delete).to do
          #receive(:execute)
        #end
        view.remove
      end
    end
  end

  describe '#remove_one' do

    context 'when sort has been specified' do

      it 'raises an exception' do
        expect{ view.sort({ :a => 1}).remove }.to raise_error
      end
    end

    context 'when skip has been specified' do

      it 'raises an exception' do
        expect{ view.skip(2).remove }.to raise_error
      end
    end

    context 'delete operation' do

      it 'creates a delete operation with the correct spec' do
        # @todo check spec
        #expect(Mongo::Operation::Delete).to receive(:new)
        view.remove
      end

      it 'executes a delete operation' do
        #expect_any_instance_of(Mongo::Operation::Delete).to do
          #receive(:execute)
        #end
        view.remove
      end
    end
  end

  describe '#replace_one' do
    let(:replacement) { { :name => 'Emily' } }
    let(:bad_replacement) { { :$name => 'Emily' } }

    context 'when sort has been specified' do

      it 'raises an exception' do
        expect{ view.sort({ :a => 1}).replace_one(replacement) }.to raise_error
      end
    end

    context 'when skip has been specified' do

      it 'raises an exception' do
        expect{ view.skip(2).replace_one(replacement) }.to raise_error
      end
    end

    context 'replacement doc whose first key does not begin with $' do

      it 'succeeds' do
        expect(view.replace_one(replacement)).to be_true
      end
    end

    context 'replacement doc whose first key begins with $' do

      it 'raises an exception' do
        expect{ view.replace_one(bad_replacement) }.to raise_error
      end
    end

    context 'update operation' do

      it 'creates an update operation with the correct spec' do
        # @todo check spec, that multi is false
        #expect(Mongo::Operation::Update).to receive(:new)
        view.replace_one(replacement)
      end

      it 'executes an update operation' do
        #expect_any_instance_of(Mongo::Operation::Update).to do
          #receive(:execute)
        #end
        view.replace_one(replacement)
      end
    end
  end

  describe '#update' do
    let(:update) { { :$set => { :name => "Emily" } } }
    let(:bad_update) { { :name => "Emily" } }

    context 'when sort has been specified' do

      it 'raises an exception' do
        expect{ view.sort({ :a => 1}).update(update) }.to raise_error
      end
    end

    context 'when skip has been specified' do

      it 'raises an exception' do
        expect{ view.skip(2).update(update) }.to raise_error
      end
    end

    context 'update doc whose first key begins with $' do

      it 'succeeds' do
        expect(view.update(update)).to be_true
      end
    end

    context 'update doc whose first key does not begin with $' do

      it 'raises an exception' do
        expect{ view.update(bad_update) }.to raise_error
      end
    end

    context 'update operation' do

      it 'creates an update operation with the correct spec' do
        # @todo check spec, that multi is true
        #expect(Mongo::Operation::Update).to receive(:new)
        view.update(update)
      end

      it 'executes an update operation' do
        #expect_any_instance_of(Mongo::Operation::Update).to do
          #receive(:execute)
        #end
        view.update(update)
      end
    end
  end

  describe '#update_one' do
    let(:update) { { :$set => { :name => "Emily" } } }
    let(:bad_update) { { :name => "Emily" } }

    context 'when sort has been specified' do

      it 'raises an exception' do
        expect{ view.sort({ :a => 1}).update(update) }.to raise_error
      end
    end

    context 'when skip has been specified' do

      it 'raises an exception' do
        expect{ view.skip(2).update(update) }.to raise_error
      end
    end

    context 'update doc whose first key begins with $' do

      it 'succeeds' do
        expect(view.update(update)).to be_true
      end
    end

    context 'update doc whose first key does not begin with $' do

      it 'raises an exception' do
        expect{ view.update(bad_update) }.to raise_error
      end
    end

    context 'update operation' do

      it 'creates an update operation with the correct spec' do
        # @todo check spec, that multi is false
        #expect(Mongo::Operation::Update).to receive(:new)
        view.update(update)
      end

      it 'executes an update operation' do
        #expect_any_instance_of(Mongo::Operation::Update).to do
          #receive(:execute)
        #end
        view.update(update)
      end
    end
  end
end
