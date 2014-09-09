require 'spec_helper'

describe Mongo::Operation::Write::Delete do
  include_context 'operation'

  let(:delete) { [{:q => { :foo => 1 }, :limit => 1}] }
  let(:spec) do
    { :delete        => delete,
      :db_name       => db_name,
      :coll_name     => coll_name,
      :write_concern => write_concern
    }
  end

  let(:op) { described_class.new(spec) }

  describe '#initialize' do

    context 'spec' do

      it 'sets the spec' do
        expect(op.spec).to eq(spec)
      end
    end
  end

  describe '#==' do

    context 'spec' do

      context 'when two ops have the same specs' do
        let(:other) { described_class.new(spec) }

        it 'returns true' do
          expect(op).to eq(other)
        end
      end

      context 'when two ops have different specs' do
        let(:other_delete) { {:q => { :bar => 1 }, :limit => 1} }
        let(:other_spec) do
          { :delete        => other_delete,
            :db_name       => db_name,
            :coll_name     => coll_name,
            :write_concern => write_concern
          }
        end
        let(:other) { described_class.new(other_spec) }

        it 'returns false' do
          expect(op).not_to eq(other)
        end
      end
    end
  end

  describe '#execute' do

    before do
      authorized_collection.insert_many([
        { name: 'test', field: 'test' },
        { name: 'testing', field: 'test' }
      ])
    end

    after do
      authorized_collection.find.remove_many
    end

    context 'when deleting a single document' do

      let(:delete) do
        described_class.new({
          delete: delete_doc,
          db_name: TEST_DB,
          coll_name: TEST_COLL,
          write_concern: Mongo::WriteConcern::Mode.get(:w => 1)
        })
      end

      context 'when the delete succeeds' do

        let(:delete_doc) do
          { q: { field: 'test' }, limit: 1 }
        end

        let(:result) do
          delete.execute(authorized_primary.context)
        end

        it 'deletes the document from the database' do
          expect(result.n).to eq(1)
        end
      end

      context 'when the delete fails' do

        let(:delete_doc) do
          { que: { field: 'test' }}
        end

        it 'raises an exception' do
          expect {
            delete.execute(authorized_primary.context)
          }.to raise_error(Mongo::Operation::Write::Failure)
        end
      end
    end

    context 'when the server is a secondary' do

      pending 'it raises an exception'
    end
  end
end
