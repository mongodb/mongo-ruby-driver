require 'spec_helper'

describe Mongo::Operation::Write::Delete do
  include_context 'operation'

  let(:deletes) { [{:q => { :foo => 1 }, :limit => 1}] }
  let(:spec) do
    { :deletes       => deletes,
      :db_name       => db_name,
      :coll_name     => coll_name,
      :write_concern => write_concern,
      :ordered       => true
    }
  end

  let(:delete_write_cmd) do
    double('delete_write_cmd').tap do |d|
      allow(d).to receive(:execute) { [] }
    end
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
        let(:other_deletes) { [{:q => { :bar => 1 }, :limit => 1}] }
        let(:other_spec) do
          { :deletes       => other_deletes,
            :db_name       => db_name,
            :coll_name     => coll_name,
            :write_concern => write_concern,
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
        expect(copy.spec[:deletes]).not_to be(op.spec[:deletes])
      end
    end
  end

  describe '#merge' do

    context 'same collection and database' do
      let(:other_deletes) { [{:q => { :bar => 1 }, :limit => 1}] }
      let(:other_spec) do
        { :deletes       => other_deletes,
          :db_name       => db_name,
          :coll_name     => coll_name
        }
      end
      let(:other) { described_class.new(other_spec) }

      it 'merges the two ops' do
        expect{ op.merge(other) }.not_to raise_exception
      end
    end

    context 'different database' do
      let(:other_deletes) { [{:q => { :bar => 1 }, :limit => 1}] }
      let(:other_spec) do
        { :deletes       => other_deletes,
          :db_name       => 'different',
          :coll_name     => coll_name
        }
      end
      let(:other) { described_class.new(other_spec) }

      it 'raises an exception' do
        expect{ op.merge(other) }.to raise_exception
      end
    end

    context 'different collection' do
      let(:other_deletes) { [{:q => { :bar => 1 }, :limit => 1}] }
      let(:other_spec) do
        { :deletes       => other_deletes,
          :db_name       => db_name,
          :coll_name     => 'different'
        }
      end
      let(:other) { described_class.new(other_spec) }

      it 'raises an exception' do
        expect{ op.merge(other) }.to raise_exception
      end
    end

    context 'different operation type' do
      let(:other) { Mongo::Write::Update.new(spec) }

      it 'raises an exception' do
        expect{ op.merge(other) }.to raise_exception
      end
    end

    context 'merged deletes' do
      let(:other_deletes) { [{:q => { :bar => 1 }, :limit => 1}] }
      let(:other_spec) do
        { :deletes       => other_deletes,
          :db_name       => db_name,
          :coll_name     => coll_name
        }
      end
      let(:other) { described_class.new(other_spec) }
      let(:expected) { deletes << other_deletes }

      it 'merges the list of deletes' do
        expect(op.merge(other).spec[:deletes]).to eq(expected)
      end
    end

    context 'mutability' do
      let(:other_deletes) { [{:q => { :bar => 1 }, :limit => 1}] }
      let(:other_spec) do
        { :deletes       => other_deletes,
          :db_name       => db_name,
          :coll_name     => coll_name
        }
      end
      let(:other) { described_class.new(other_spec) }

      it 'returns a new object' do
        expect(op.merge(other)).not_to be(op)
      end
    end
  end

  describe '#merge!' do
    context 'same collection and database' do
      let(:other_deletes) { [{:q => { :bar => 1 }, :limit => 1}] }
      let(:other_spec) do
        { :deletes       => other_deletes,
          :db_name       => db_name,
          :coll_name     => coll_name
        }
      end
      let(:other) { described_class.new(other_spec) }

      it 'merges the two ops' do
        expect{ op.merge!(other) }.not_to raise_exception
      end
    end

    context 'different database' do
      let(:other_deletes) { [{:q => { :bar => 1 }, :limit => 1}] }
      let(:other_spec) do
        { :deletes       => other_deletes,
          :db_name       => 'different',
          :coll_name     => coll_name
        }
      end
      let(:other) { described_class.new(other_spec) }

      it 'raises an exception' do
        expect{ op.merge!(other) }.to raise_exception
      end
    end

    context 'different collection' do
      let(:other_deletes) { [{:q => { :bar => 1 }, :limit => 1}] }
      let(:other_spec) do
        { :deletes       => other_deletes,
          :db_name       => db_name,
          :coll_name     => 'different'
        }
      end
      let(:other) { described_class.new(other_spec) }

      it 'raises an exception' do
        expect{ op.merge!(other) }.to raise_exception
      end
    end

    context 'different operation type' do
      let(:other) { Mongo::Write::Update.new(spec) }

      it 'raises an exception' do
        expect{ op.merge!(other) }.to raise_exception
      end
    end

    context 'merged deletes' do
      let(:other_deletes) { [{:q => { :bar => 1 }, :limit => 1}] }
      let(:other_spec) do
        { :deletes       => other_deletes,
          :db_name       => db_name,
          :coll_name     => coll_name
        }
      end
      let(:other) { described_class.new(other_spec) }
      let(:expected) { deletes << other_deletes }

      it 'merges the list of deletes' do
        expect(op.merge!(other).spec[:deletes]).to eq(expected)
      end
    end

    context 'mutability' do
      let(:other_deletes) { [{:q => { :bar => 1 }, :limit => 1}] }
      let(:other_spec) do
        { :deletes       => other_deletes,
          :db_name       => db_name,
          :coll_name     => coll_name
        }
      end
      let(:other) { described_class.new(other_spec) }

      it 'mutates the object itself' do
        expect(op.merge!(other)).to be(op)
      end
    end
  end

  describe '#slice' do

    context 'number of deletes is evenly divisible by divisor' do
      let(:deletes) do
        [ {:q => { :a => 1 } },
          {:q => { :b => 1 } },
          {:q => { :c => 1 } },
          {:q => { :d => 1 } },
          {:q => { :e => 1 } },
          {:q => { :f => 1 } } ]
      end
      let(:divisor) { 3 }

      it 'slices the op into the divisor number of children ops' do
        expect(op.slice(divisor).size).to eq(divisor)
      end

      it 'divides the deletes evenly between children ops' do
        ops = op.slice(divisor)
        slice_size = deletes.size / divisor

        divisor.times do |i|
          start_index = i * slice_size
          expect(ops[i].spec[:deletes]).to eq(deletes[start_index, slice_size])
        end
      end
    end

    context 'number of deletes is not evenly divisible by divisor' do
      let(:deletes) do
        [ {:q => { :a => 1 } },
          {:q => { :b => 1 } },
          {:q => { :c => 1 } },
          {:q => { :d => 1 } },
          {:q => { :e => 1 } },
          {:q => { :f => 1 } } ]
      end
      let(:divisor) { 4 }

      it 'slices the op into the divisor number of children ops' do
        expect(op.slice(divisor).size).to eq(divisor)
      end

      it 'divides the deletes evenly between children ops' do
        ops = op.slice(divisor)
        slice_size = deletes.size / divisor

        divisor.times do |i|
          start_index = i * slice_size
          if i == divisor - 1
            expect(ops[i].spec[:deletes]).to eq(deletes[start_index..-1])
          else
            expect(ops[i].spec[:deletes]).to eq(deletes[start_index, slice_size])
          end
        end
      end
    end
  end

  describe '#set_order' do

    context 'when an order has been set' do
      let(:order) { 5 }
      let(:deletes) do
        [ {:q => { :a => 1 } },
          {:q => { :b => 1 } },
          {:q => { :c => 1 } } ]
      end
      let(:expected) do
        [ {:q => { :a => 1 }, :ord => order },
          {:q => { :b => 1 }, :ord => order },
          {:q => { :c => 1 }, :ord => order } ]
      end

      it 'sets the order on each op spec document' do
        op.set_order(order)
        expect(op.spec[:deletes]).to eq(expected)
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
        :documents     => [{ name: 'test', field: 'test' }, { name: 'testing', field: 'test' }],
        :db_name       => TEST_DB,
        :coll_name     => TEST_COLL,
        :write_concern => Mongo::WriteConcern::Mode.get(:w => 1)
      }).execute(server.context)
    end

    after do
      described_class.new({
        deletes: [{ q: {}, limit: -1 }],
        db_name: TEST_DB,
        coll_name: TEST_COLL,
        write_concern: Mongo::WriteConcern::Mode.get(:w => 1)
      }).execute(server.context)
    end

    context 'when deleting a single document' do

      let(:delete) do
        described_class.new({
          deletes: documents,
          db_name: TEST_DB,
          coll_name: TEST_COLL,
          write_concern: Mongo::WriteConcern::Mode.get(:w => 1)
        })
      end

      context 'when the delete succeeds' do

        let(:documents) do
          [{ q: { field: 'test' }, limit: 1 }]
        end

        let(:result) do
          delete.execute(server.context)
        end

        it 'deletes the documents from the database' do
          expect(result.n).to eq(1)
        end
      end

      context 'when the delete fails' do

        let(:documents) do
          [{ que: { field: 'test' }}]
        end

        it 'raises an exception' do
          expect {
            delete.execute(server.context)
          }.to raise_error(Mongo::Operation::Write::Failure)
        end
      end
    end

    context 'when deleting multiple documents' do

      let(:delete) do
        described_class.new({
          deletes: documents,
          db_name: TEST_DB,
          coll_name: TEST_COLL,
          write_concern: Mongo::WriteConcern::Mode.get(:w => 1)
        })
      end

      context 'when the deletes succeed' do

        let(:documents) do
          [{ q: { field: 'test' }, limit: -1 }]
        end

        let(:result) do
          delete.execute(server.context)
        end

        it 'deletes the documents from the database' do
          expect(result.n).to eq(2)
        end
      end

      context 'when a delete fails' do

        let(:documents) do
          [{ q: { field: 'tester' }, limit: -1 }]
        end

        let(:result) do
          delete.execute(server.context)
        end

        it 'does not delete any documents' do
          expect(result.n).to eq(0)
        end
      end
    end

    context 'when the server is a secondary' do

      pending 'it raises an exception'
    end
  end
end
