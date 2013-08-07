require 'spec_helper'

describe Mongo::Operation::FindAndModify do

  include_context 'shared find_and_modify'

  describe 'opts' do

    context 'update' do

      context 'when the first key in the update doc begins with $' do
        let(:fm_opts) { { :update => { :$set => { :f_name => 'Emilie' } } } }

        it 'does not raise an exception' do
          expect { fm_op }.not_to raise_error
        end
      end

      context 'when the first key in the update doc does not begin with $' do
        let(:fm_opts) { { :update => { :set => { :f_name => 'Emilie' } } } }

        it 'raises an exception' do
          expect { fm_op }.to raise_error
        end
      end
    end

    context 'replace' do

      context 'when the first key in replacement doc does not begin with $' do
        let(:fm_opts) { { :replace => { :f_name => 'Emilie' } } }

        it 'does not raise an exception' do
          expect { fm_op }.not_to raise_error
        end
      end

      context 'when the first key in the replacement doc begins with $' do
        let(:fm_opts) { { :replace => { :$set => { :f_name => 'Emilie' } } } }

        it 'raises an exception' do
          expect { fm_op }.to raise_error
        end
      end
    end

    context 'skip' do

      context 'when skip is not specified' do
        it 'does not raise an exception' do
          expect { fm_op }.not_to raise_error
        end
      end

      context 'when skip is specified' do
        let(:scope_opts) { { :skip => 10 } }

        it 'raises an exception' do
          expect { fm_op }.to raise_error
        end
      end
    end
  end

  describe '#execute' do

    it 'uses the correct collection name' do
      expect(cluster).to receive(:execute) do |op|
        expect(op[:findandmodify]).to eq(TEST_COLL)
        op
      end
      fm_op.execute
    end

    it 'sets the query to the scope selector' do
      expect(cluster).to receive(:execute) do |op|
        expect(op[:query]).to eq(scope.selector)
        op
      end
      fm_op.execute
    end

    it 'sends the command to the database to be executed' do
      expect(cluster).to receive(:execute)
      fm_op.execute
    end

    context 'when there is a doc returned in the value field' do
      it 'returns the doc' do
        expect(fm_op.execute).to eq(value)
      end
    end

    context 'when null is returned in the value field' do
      let(:value) { 'null' }

      it 'returns nil' do
        expect(fm_op.execute).to be_nil
      end
    end

    context 'new' do

      context 'when new is true' do
        let(:fm_opts) { { :new => true } }

        it 'sets new to true' do
          expect(cluster).to receive(:execute) do |op|
            expect(op[:new]).to be_true
            op
          end
          fm_op.execute
        end
      end

      context 'when new is false' do
        let(:fm_opts) { { :new => false } }

        it 'sets new to false' do
          expect(cluster).to receive(:execute) do |op|
            expect(op[:new]).to be_false
            op
          end
          fm_op.execute
        end
      end

      context 'when new is not specified' do
        it 'sets new to false' do
          expect(cluster).to receive(:execute) do |op|
            expect(op[:new]).to be_false
            op
          end
          fm_op.execute
        end
      end
    end

    context 'fields' do
      let(:fm_opts) { { :fields => { :f_name => 1, :_id => 0 } } }

      context 'when there are fields specified' do
        it 'sets the fields' do
          expect(cluster).to receive(:execute) do |op|
            expect(op[:fields]).to eq(fm_opts[:fields])
            op
          end
          fm_op.execute
        end
      end
    end

    context 'upsert' do

      context 'when upsert is true' do
        let(:fm_opts) { { :upsert => true } }

        it 'sets upsert to true' do
          expect(cluster).to receive(:execute) do |op|
            expect(op[:upsert]).to be_true
            op
          end
          fm_op.execute
        end
      end

      context 'when upsert is false' do
        let(:fm_opts) { { :upsert => false } }

        it 'sets upsert to false' do
          expect(cluster).to receive(:execute) do |op|
            expect(op[:upsert]).to be_false
            op
          end
          fm_op.execute
        end
      end

      context 'when upsert is not specified' do
        it 'sets upsert to false' do
          expect(cluster).to receive(:execute) do |op|
            expect(op[:upsert]).to be_false
            op
          end
          fm_op.execute
        end
      end
    end

    context 'sort' do

      context 'when there is a sort specified' do
        let(:scope_opts) { { :sort => { :f_name => 1 } } }

        it 'sets the sort' do
          expect(cluster).to receive(:execute) do |op|
            expect(op[:sort]).to eq(scope_opts[:sort])
            op
          end
          fm_op.execute
        end
      end
    end

    context 'update' do

      context 'when there is an update specified' do
        let(:fm_opts) { { :update => update } }

        it 'sets the update key to the update doc' do
          expect(cluster).to receive(:execute) do |op|
            expect(op[:update]).to eq(update)
            op
          end
          fm_op.execute
        end
      end
    end

    context 'replace' do

      context 'when there is a replacement specified' do
        let(:fm_opts) { { :replace => replacement } }

        it 'sets the update key to the replacement doc' do
          expect(cluster).to receive(:execute) do |op|
            expect(op[:update]).to eq(replacement)
            op
          end
          fm_op.execute
        end
      end
    end

    context 'remove' do

      context 'when remove is true' do
        let(:fm_opts) { { :remove => true } }

        it 'sets remove to true' do
          expect(cluster).to receive(:execute) do |op|
            expect(op[:remove]).to be_true
            op
          end
          fm_op.execute
        end
      end

      context 'when remove is false' do
        let(:fm_opts) { { :remove => false } }

        it 'sets remove to false' do
          expect(cluster).to receive(:execute) do |op|
            expect(op[:remove]).to be_false
            op
          end
          fm_op.execute
        end
      end

      context 'when remove is not specified' do
        it 'sets remove to false' do
          expect(cluster).to receive(:execute) do |op|
            expect(op[:remove]).to be_false
            op
          end
          fm_op.execute
        end
      end
    end
  end
end
