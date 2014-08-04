require 'spec_helper'

describe Mongo::Operation::Write::Update do

  let(:updates) do
    [{ :q => { :foo => 1 },
       :u => { :$set => { :bar => 1 } },
       :multi => true,
       :upsert => false }]
  end

  let(:spec) do
    { :updates       => updates,
      :db_name       => TEST_DB,
      :coll_name     => TEST_COLL,
      :write_concern => Mongo::WriteConcern::Mode.get(:w => 1),
      :ordered       => true
    }
  end

  let(:update) do
    described_class.new(spec)
  end

  describe '#initialize' do

    context 'spec' do

      it 'sets the spec' do
        expect(update.spec).to eq(spec)
      end
    end
  end

  describe '#==' do

    context 'spec' do

      context 'when two ops have the same specs' do

        let(:other) { described_class.new(spec) }

        it 'returns true' do
          expect(update).to eq(other)
        end
      end

      context 'when two ops have different specs' do
        let(:other_updates) { [{:q => { :foo => 1 },
                                :u => { :$set => { :bar => 1 } },
                                :multi => true,
                                :upsert => true }] }
        let(:other_spec) do
          { :updates       => other_updates,
            :db_name       => TEST_DB,
            :coll_name     => TEST_COLL,
            :write_concern => Mongo::WriteConcern::Mode.get(:w => 1),
            :ordered       => true
          }
        end

        let(:other) { described_class.new(other_spec) }

        it 'returns false' do
          expect(update).not_to eq(other)
        end
      end
    end
  end

  describe '#dup' do

    context 'deep copy' do

      it 'copies the list of updates' do
        copy = update.dup
        expect(copy.spec[:updates]).not_to be(update.spec[:updates])
      end
    end
  end

  describe '#merge' do

    context 'same collection and database' do

      let(:other_updates) do
        [{:q => { :foo => 1 },
          :u => { :$set => { :bar => 1 } },
          :multi => true,
          :upsert => true }]
      end

      let(:other_spec) do
        { :updates       => other_updates,
          :db_name       => TEST_DB,
          :coll_name     => TEST_COLL
        }
      end
      let(:other) { described_class.new(other_spec) }

      it 'merges the two ops' do
        expect{ update.merge(other) }.not_to raise_exception
      end
    end

    context 'different database' do
      let(:other_updates) { [{:q => { :foo => 1 },
                              :u => { :$set => { :bar => 1 } },
                              :multi => true,
                              :upsert => true }] }
      let(:other_spec) do
        { :updates       => other_updates,
          :db_name       => 'different',
          :coll_name     => TEST_COLL
        }
        end
      let(:other) { described_class.new(other_spec) }

      it 'raises an exception' do
        expect{ op.merge(other) }.to raise_exception
      end
    end

    context 'different collection' do
      let(:other_updates) { [{:q => { :foo => 1 },
                              :u => { :$set => { :bar => 1 } },
                              :multi => true,
                              :upsert => true }] }
      let(:other_spec) do
        { :updates       => other_updates,
          :db_name       => TEST_DB,
          :coll_name     => 'different'
        }
      end
      let(:other) { described_class.new(other_spec) }

      it 'raises an exception' do
        expect{ op.merge(other) }.to raise_exception
      end
    end

    context 'different operation type' do
      let(:other) { Mongo::Write::Insert.new(spec) }

      it 'raises an exception' do
        expect{ op.merge(other) }.to raise_exception
      end
    end

    context 'merged updates' do
      let(:other_updates) { [{:q => { :foo => 1 },
                              :u => { :$set => { :bar => 1 } },
                              :multi => true,
                              :upsert => true }] }
      let(:other_spec) do
        { :updates       => other_updates,
          :db_name       => TEST_DB,
          :coll_name     => TEST_COLL
        }
      end
      let(:other) { described_class.new(other_spec) }
      let(:expected) { updates << other_updates }

      it 'merges the list of deletes' do
        expect(update.merge(other).spec[:updates]).to eq(expected)
      end
    end

    context 'mutability' do
      let(:other_updates) { [{:q => { :foo => 1 },
                              :u => { :$set => { :bar => 1 } },
                              :multi => true,
                              :upsert => true }] }
      let(:other_spec) do
        { :updates       => other_updates,
          :db_name       => TEST_DB,
          :coll_name     => TEST_COLL
        }
      end
      let(:other) { described_class.new(other_spec) }

      it 'returns a new object' do
        expect(update.merge(other)).not_to be(update)
      end
    end
  end

  describe '#merge!' do

    context 'same collection and database' do
      let(:other_updates) { [{:q => { :foo => 1 },
                              :u => { :$set => { :bar => 1 } },
                              :multi => true,
                              :upsert => true }] }
      let(:other_spec) do
        { :updates       => other_updates,
          :db_name       => TEST_DB,
          :coll_name     => TEST_COLL
        }
      end
      let(:other) { described_class.new(other_spec) }

      it 'merges the two ops' do
        expect{ update.merge!(other) }.not_to raise_exception
      end
    end

    context 'different database' do
      let(:other_updates) { [{:q => { :foo => 1 },
                              :u => { :$set => { :bar => 1 } },
                              :multi => true,
                              :upsert => true }] }
      let(:other_spec) do
        { :updates       => other_updates,
          :db_name       => 'different',
          :coll_name     => TEST_COLL
        }
      end
      let(:other) { described_class.new(other_spec) }

      it 'raises an exception' do
        expect{ update.merge!(other) }.to raise_exception
      end
    end

    context 'different collection' do
      let(:other_updates) { [{:q => { :foo => 1 },
                              :u => { :$set => { :bar => 1 } },
                              :multi => true,
                              :upsert => true }] }
      let(:other_spec) do
        { :updates       => other_updates,
          :db_name       => TEST_DB,
          :coll_name     => 'different'
        }
      end
      let(:other) { described_class.new(other_spec) }

      it 'raises an exception' do
        expect{ update.merge!(other) }.to raise_exception
      end
    end

    context 'different operation type' do
      let(:other) { Mongo::Write::Insert.new(spec) }

      it 'raises an exception' do
        expect{ update.merge!(other) }.to raise_exception
      end
    end

    context 'merged updates' do
      let(:other_updates) { [{:q => { :foo => 1 },
                              :u => { :$set => { :bar => 1 } },
                              :multi => true,
                              :upsert => true }] }
      let(:other_spec) do
        { :updates       => other_updates,
          :db_name       => TEST_DB,
          :coll_name     => TEST_COLL
        }
      end
      let(:other) { described_class.new(other_spec) }
      let(:expected) { updates << other_updates }

      it 'merges the list of updates' do
        expect(update.merge!(other).spec[:updates]).to eq(expected)
      end
    end

    context 'mutability' do
      let(:other_updates) { [{:q => { :foo => 1 },
                              :u => { :$set => { :bar => 1 } },
                              :multi => true,
                              :upsert => true }] }
      let(:other_spec) do
        { :updates       => other_updates,
          :db_name       => TEST_DB,
          :coll_name     => TEST_COLL
        }
      end
      let(:other) { described_class.new(other_spec) }

      it 'mutates the operation itself' do
        expect(update.merge!(other)).to be(update)
      end
    end
  end

  describe '#slice' do

    context 'number of updates is evenly divisible by divisor' do
      let(:updates) do
        [{ :q => { :a => 1 },
           :u => { :$set => { :a => 2 } },
           :multi => true,
           :upsert => false },
         { :q => { :b => 1 },
           :u => { :$set => { :b => 2 } },
           :multi => true,
           :upsert => false },
         { :q => { :c => 1 },
           :u => { :$set => { :c => 2 } },
           :multi => true,
           :upsert => false },
         { :q => { :d => 1 },
           :u => { :$set => { :d => 2 } },
           :multi => true,
           :upsert => false },
         { :q => { :e => 1 },
           :u => { :$set => { :e => 2 } },
           :multi => true,
           :upsert => false },
         { :q => { :f => 1 },
           :u => { :$set => { :f => 2 } },
           :multi => true,
           :upsert => false }
        ]
      end
      let(:divisor) { 3 }

      it 'splits the op into the divisor number of children ops' do
        expect(update.slice(divisor).size).to eq(divisor)
      end

      it 'divides the updates evenly between children ops' do
        ops = update.slice(divisor)
        slice_size = updates.size / divisor

        divisor.times do |i|
          start_index = i * slice_size
          expect(ops[i].spec[:updates]).to eq(updates[start_index, slice_size])
        end
      end
    end

    context 'number of updates is not evenly divisible by divisor' do
      let(:updates) do
        [{ :q => { :a => 1 },
           :u => { :$set => { :a => 2 } },
           :multi => true,
           :upsert => false },
         { :q => { :b => 1 },
           :u => { :$set => { :b => 2 } },
           :multi => true,
           :upsert => false },
         { :q => { :c => 1 },
           :u => { :$set => { :c => 2 } },
           :multi => true,
           :upsert => false },
         { :q => { :d => 1 },
           :u => { :$set => { :d => 2 } },
           :multi => true,
           :upsert => false },
         { :q => { :e => 1 },
           :u => { :$set => { :e => 2 } },
           :multi => true,
           :upsert => false },
         { :q => { :f => 1 },
           :u => { :$set => { :f => 2 } },
           :multi => true,
           :upsert => false }
        ]
      end
      let(:divisor) { 4 }

      it 'splits the op into the divisor number of children ops' do
        expect(update.slice(divisor).size).to eq(divisor)
      end

      it 'divides the updates evenly between children ops' do
        ops = update.slice(divisor)
        slice_size = updates.size / divisor

        divisor.times do |i|
          start_index = i * slice_size
          if i == divisor - 1
            expect(ops[i].spec[:updates]).to eq(updates[start_index..-1])
          else
            expect(ops[i].spec[:updates]).to eq(updates[start_index, slice_size])
          end
        end
      end
    end
  end

  describe '#set_order' do

    context 'when an order has been set' do
      let(:order) { 5 }
      let(:updates) do
        [{ :q => { :a => 1 },
           :u => { :$set => { :a => 2 } },
           :multi => true,
           :upsert => false },
         { :q => { :b => 1 },
           :u => { :$set => { :b => 2 } },
           :multi => true,
           :upsert => false },
         { :q => { :c => 1 },
           :u => { :$set => { :c => 2 } },
           :multi => true,
           :upsert => false } ]
      end
      let(:expected) do
        [{ :q => { :a => 1 },
           :u => { :$set => { :a => 2 } },
           :multi => true,
           :upsert => false,
           :ord => order },
         { :q => { :b => 1 },
           :u => { :$set => { :b => 2 } },
           :multi => true,
           :upsert => false,
           :ord => order },
         { :q => { :c => 1 },
           :u => { :$set => { :c => 2 } },
           :multi => true,
           :upsert => false,
           :ord => order } ]
      end

      it 'sets the order on each op spec document' do
        update.set_order(order)
        expect(update.spec[:updates]).to eq(expected)
      end
    end
  end

  describe '#execute' do

    let(:client) do
      Mongo::Client.new(
        [ '127.0.0.1:27017' ],
        database: TEST_DB,
        username: ROOT_USER.name,
        password: ROOT_USER.password
      )
    end

    let(:server) do
      client.cluster.servers.first
    end

    before do
      client.cluster.scan!
      Mongo::Operation::Write::Insert.new({
        :documents     => [
          { name: 'test', field: 'test', other: 'test' },
          { name: 'testing', field: 'test', other: 'test' }
        ],
        :db_name       => TEST_DB,
        :coll_name     => TEST_COLL,
        :write_concern => Mongo::WriteConcern::Mode.get(:w => 1)
      }).execute(server.context)
    end

    after do
      Mongo::Operation::Write::Delete.new({
        deletes: [{ q: {}, limit: -1 }],
        db_name: TEST_DB,
        coll_name: TEST_COLL,
        write_concern: Mongo::WriteConcern::Mode.get(:w => 1)
      }).execute(server.context)
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
          update.execute(server.context)
        end

        it 'updates the document' do
          expect(result.n).to eq(1)
        end
      end

      context 'when the update fails' do

        let(:documents) do
          [{ q: { name: 'test' }, u: { '$st' => { field: 'blah' }}}]
        end

        it 'raises an exception' do
          expect {
            update.execute(server.context)
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
          update.execute(server.context)
        end

        it 'updates the documents' do
          expect(result.n).to eq(2)
        end
      end

      context 'when an update fails' do

        let(:documents) do
          [{ q: { name: 'test' }, u: { '$st' => { field: 'blah' }}, multi: true}]
        end

        it 'raises an exception' do
          expect {
            update.execute(server.context)
          }.to raise_error(Mongo::Operation::Write::Failure)
        end
      end
    end

    context 'when the server is a secondary' do

      pending 'it raises an exception'
    end
  end
end
