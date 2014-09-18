require 'spec_helper'

describe Mongo::Operation::Write::BulkUpdate do

  let(:documents) do
    [{ :q => { :foo => 1 },
       :u => { :$set => { :bar => 1 } },
       :multi => true,
       :upsert => false }]
  end

  let(:spec) do
    { :updates       => documents,
      :db_name       => TEST_DB,
      :coll_name     => TEST_COLL,
      :write_concern => Mongo::WriteConcern::Mode.get(:w => 1),
      :ordered       => true
    }
  end

  let(:op) do
    described_class.new(spec)
  end

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
        let(:other_docs) do 
          [ {:q => { :foo => 1 },
             :u => { :$set => { :bar => 1 } },
             :multi => true,
             :upsert => true } ]
        end

        let(:other_spec) do
          { :updates       => other_docs,
            :db_name       => TEST_DB,
            :coll_name     => TEST_COLL,
            :write_concern => Mongo::WriteConcern::Mode.get(:w => 1),
            :ordered       => true
          }
        end

        let(:other) { described_class.new(other_spec) }

        it 'returns false' do
          expect(op).not_to eq(other)
        end
      end
    end
  end

  describe '#dup' do

    context 'deep copy' do

      it 'copies the list of updates' do
        copy = op.dup
        expect(copy.spec[:updates]).not_to be(op.spec[:updates])
      end
    end
  end

  describe '#execute' do

    before do
      authorized_collection.insert_many([
        { name: 'test', field: 'test', other: 'test' },
        { name: 'testing', field: 'test', other: 'test' }
      ])
    end

    after do
      authorized_collection.find.remove_many
    end

    context 'when updating a single document' do

      let(:update) do
        described_class.new({
          updates: documents,
          db_name: TEST_DB,
          coll_name: TEST_COLL,
          write_concern: Mongo::WriteConcern::Mode.get(:w => 1)
        })
      end

      context 'when the update passes' do

        let(:documents) do
          [{ q: { name: 'test' }, u: { '$set' => { field: 'blah' }}, limit: 1 }]
        end

        let(:result) do
          op.execute(authorized_primary.context)
        end

        it 'updates the document' do
          expect(result.written_count).to eq(1)
        end
      end

      context 'when the update fails' do

        let(:documents) do
          [{ q: { name: 'test' }, u: { '$st' => { field: 'blah' }}}]
        end

        it 'raises an exception' do
          expect {
            op.execute(authorized_primary.context)
          }.to raise_error(Mongo::Operation::Write::Failure)
        end
      end
    end

    context 'when updating multiple documents' do

      let(:update) do
        described_class.new({
          updates: documents,
          db_name: TEST_DB,
          coll_name: TEST_COLL,
          write_concern: Mongo::WriteConcern::Mode.get(:w => 1)
        })
      end

      context 'when the updates succeed' do

        let(:documents) do
          [{ q: { field: 'test' }, u: { '$set' => { other: 'blah' }}, multi: true }]
        end

        let(:result) do
          op.execute(authorized_primary.context)
        end

        it 'updates the documents' do
          expect(result.written_count).to eq(2)
        end
      end

      context 'when an update fails' do

        let(:documents) do
          [{ q: { name: 'test' }, u: { '$st' => { field: 'blah' }}, multi: true}]
        end

        it 'raises an exception' do
          expect {
            op.execute(authorized_primary.context)
          }.to raise_error(Mongo::Operation::Write::Failure)
        end
      end
    end

    context 'when the updates are ordered' do

      let(:documents) do
        [ { q: { name: 'test' }, u: { '$st' => { field: 'blah' }}, multi: true},
          { q: { field: 'test' }, u: { '$set' => { other: 'blah' }}, multi: true }
        ]
      end

      let(:spec) do
        { :updates       => documents,
          :db_name       => TEST_DB,
          :coll_name     => TEST_COLL,
          :write_concern => Mongo::WriteConcern::Mode.get(w: 1),
          :ordered       => true
        }
      end

      let(:failing_update) do
        described_class.new(spec)
      end
  
      it 'aborts after first error' do
        expect {
          failing_update.execute(authorized_primary.context)
        }.to raise_error(Mongo::Operation::Write::Failure)
        expect(authorized_collection.find(other: 'blah').count).to eq(0)
      end
    end

    context 'when the updates are unordered' do

      let(:documents) do
        [ { q: { name: 'test' }, u: { '$st' => { field: 'blah' }}, multi: true},
          { q: { field: 'test' }, u: { '$set' => { other: 'blah' }}, multi: true }
        ]
      end

      let(:spec) do
        { :updates       => documents,
          :db_name       => TEST_DB,
          :coll_name     => TEST_COLL,
          :write_concern => Mongo::WriteConcern::Mode.get(w: 1),
          :ordered       => false
        }
      end

      let(:failing_update) do
        described_class.new(spec)
      end

      it 'it continues executing operations after errors' do
        expect {
          failing_update.execute(authorized_primary.context)
        }.to raise_error(Mongo::Operation::Write::Failure)
        expect(authorized_collection.find(other: 'blah').count).to eq(2)
      end
    end

    context 'when the server is a secondary' do

      pending 'it raises an exception'
    end
  end
end