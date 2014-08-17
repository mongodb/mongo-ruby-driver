require 'spec_helper'

describe Mongo::Operation::Write::Insert do

  let(:documents) do
    [{ :name => 'test' }]
  end

  let(:spec) do
    { :documents     => documents,
      :db_name       => TEST_DB,
      :coll_name     => TEST_COLL,
      :write_concern => Mongo::WriteConcern::Mode.get(:w => 1),
      :ordered       => true
    }
  end

  let(:insert) do
    described_class.new(spec)
  end

  describe '#initialize' do

    context 'spec' do

      it 'sets the spec' do
        expect(insert.spec).to eq(spec)
      end
    end
  end

  describe '#==' do

    context 'spec' do

      context 'when two inserts have the same specs' do

        let(:other) do
          described_class.new(spec)
        end

        it 'returns true' do
          expect(insert).to eq(other)
        end
      end

      context 'when two inserts have different specs' do

        let(:other_docs) do
          [{ :bar => 1 }]
        end

        let(:other_spec) do
          { :documents     => other_docs,
            :db_name       => 'test',
            :coll_name     => 'test_coll',
            :write_concern => { 'w' => 1 },
            :ordered       => true
          }
        end

        let(:other) do
          described_class.new(other_spec)
        end

        it 'returns false' do
          expect(insert).not_to eq(other)
        end
      end
    end
  end

  describe '#dup' do

    context 'deep copy' do

      it 'copies the list of updates' do
        copy = insert.dup
        expect(copy.spec[:documents]).to_not be(insert.spec[:documents])
      end
    end
  end

  describe '#merge' do

    context 'when the collection and database are the same' do

      let(:other_docs) do
        [{ :bar => 1 }]
      end

      let(:other_spec) do
        { :documents     => other_docs,
          :db_name       => TEST_DB,
          :coll_name     => TEST_COLL
        }
      end

      let(:other) do
        described_class.new(other_spec)
      end

      it 'merges the two inserts' do
        expect{ insert.merge(other) }.not_to raise_exception
      end
    end

    context 'when the databases differ' do

      let(:other_docs) do
        [{ :bar => 1 }]
      end

      let(:other_spec) do
        { :documents     => other_docs,
          :db_name       => 'different',
          :coll_name     => TEST_COLL
        }
      end

      let(:other) do
        described_class.new(other_spec)
      end

      it 'raises an exception' do
        expect{ insert.merge(other) }.to raise_exception
      end
    end

    context 'when the collections differ' do

      let(:other_docs) do
        [{ :bar => 1 }]
      end

      let(:other_spec) do
        { :documents     => other_docs,
          :db_name       => TEST_DB,
          :coll_name     => 'different'
        }
      end

      let(:other) do
        described_class.new(other_spec)
      end

      it 'raises an exception' do
        expect{ insert.merge(other) }.to raise_exception
      end
    end

    context 'when the command types differ' do

      let(:other) do
        Mongo::Write::Update.new(spec)
      end

      it 'raises an exception' do
        expect{ insert.merge(other) }.to raise_exception
      end
    end

    context 'when the merge is valid' do

      let(:other_docs) do
        [{ :bar => 1 }]
      end

      let(:other_spec) do
        { :documents     => other_docs,
          :db_name       => TEST_DB,
          :coll_name     => TEST_COLL
        }
      end

      let(:other) do
        described_class.new(other_spec)
      end

      let(:expected) do
        documents << other_docs
      end

      it 'merges the list of documents' do
        expect(insert.merge(other).spec[:documents]).to eq(expected)
      end

      it 'keeps the original spec immutable'do
        expect(insert.merge(other)).not_to be(insert)
      end
    end
  end

  describe '#merge!' do

    context 'when collection and database are the same' do

      let(:other_docs) do
        [{ :bar => 1 }]
      end

      let(:other_spec) do
        { :documents     => other_docs,
          :db_name       => TEST_DB,
          :coll_name     => TEST_COLL
        }
      end

      let(:other) do
        described_class.new(other_spec)
      end

      it 'merges the two inserts' do
        expect{ insert.merge!(other) }.not_to raise_exception
      end
    end

    context 'when the database differs' do

      let(:other_docs) do
        [{ :bar => 1 }]
      end

      let(:other_spec) do
        { :documents     => other_docs,
          :db_name       => 'different',
          :coll_name     => TEST_COLL
        }
      end

      let(:other) do
        described_class.new(other_spec)
      end

      it 'raises an exception' do
        expect{ insert.merge!(other) }.to raise_exception
      end
    end

    context 'when the collection differs' do

      let(:other_docs) do
        [{ :bar => 1 }]
      end

      let(:other_spec) do
        { :documents     => other_docs,
          :db_name       => TEST_DB,
          :coll_name     => 'different'
        }
      end

      let(:other) do
        described_class.new(other_spec)
      end

      it 'raises an exception' do
        expect{ insert.merge!(other) }.to raise_exception
      end
    end

    context 'when the command type differs' do

      let(:other) do
        Mongo::Write::Update.new(spec)
      end

      it 'raises an exception' do
        expect{ insert.merge!(other) }.to raise_exception
      end
    end

    context 'when the commands can be merged' do

      let(:other_docs) do
        [{ :bar => 1 }]
      end

      let(:other_spec) do
        { :documents     => other_docs,
          :db_name       => TEST_DB,
          :coll_name     => TEST_COLL
        }
      end

      let(:other) do
        described_class.new(other_spec)
      end

      let(:expected) do
        documents << other_docs
      end

      it 'merges the list of documents' do
        expect(insert.merge!(other).spec[:documents]).to eq(expected)
      end

      it 'mutates the original spec' do
        expect(insert.merge!(other)).to be(insert)
      end
    end
  end

  describe '#slice' do

    context 'number of inserts is evenly divisible by divisor' do
      let(:documents) do
        [ { :a => 1 },
          { :b => 1 },
          { :c => 1 },
          { :d => 1 },
          { :e => 1 },
          { :f => 1 } ]
      end
      let(:divisor) { 3 }

      it 'slices the insert into the divisor number of children inserts' do
        expect(insert.slice(divisor).size).to eq(divisor)
      end

      it 'divides the inserts evenly between children inserts' do
        inserts = insert.slice(divisor)
        slice_size = documents.size / divisor

        divisor.times do |i|
          start_index = i * slice_size
          expect(inserts[i].spec[:documents]).to eq(documents[start_index, slice_size])
        end
      end
    end

    context 'number of inserts is not evenly divisible by divisor' do
      let(:documents) do
        [ { :a => 1 },
          { :b => 1 },
          { :c => 1 },
          { :d => 1 },
          { :e => 1 },
          { :f => 1 } ]
      end
      let(:divisor) { 4 }

      it 'slices the insert into the divisor number of children inserts' do
        expect(insert.slice(divisor).size).to eq(divisor)
      end

      it 'divides the inserts evenly between children inserts' do
        inserts = insert.slice(divisor)
        slice_size = documents.size / divisor

        divisor.times do |i|
          start_index = i * slice_size
          if i == divisor - 1
            expect(inserts[i].spec[:documents]).to eq(documents[start_index..-1])
          else
            expect(inserts[i].spec[:documents]).to eq(documents[start_index, slice_size])
          end
        end
      end
    end
  end

  describe '#set_order' do

    context 'when an order has been set' do
      let(:order) { 5 }
      let(:documents) do
        [ { :a => 1 },
          { :b => 1 },
          { :c => 1 } ]
      end
      let(:expected) do
        [ { :a => 1, :ord => order },
          { :b => 1, :ord => order },
          { :c => 1, :ord => order } ]
      end

      it 'sets the order on each insert spec document' do
        insert.set_order(order)
        expect(insert.spec[:documents]).to eq(expected)
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
      # @todo: Replace with condition variable
      client.cluster.scan!
      client[TEST_COLL].ensure_index({ name: 1 }, { unique: true })
    end

    after do
      Mongo::Operation::Write::Delete.new({
        deletes: [{ q: {}, limit: -1 }],
        db_name: TEST_DB,
        coll_name: TEST_COLL,
        write_concern: Mongo::WriteConcern::Mode.get(:w => 1)
      }).execute(server.context)
      client[TEST_COLL].drop_index({ name: 1 })
    end

    context 'when the server is a primary' do

      context 'when inserting a single document' do

        context 'when the insert succeeds' do

          let(:response) do
            insert.execute(server.context)
          end

          it 'inserts the documents into the database' do
            expect(response.n).to eq(1)
          end
        end

        context 'when the insert fails' do

          let(:documents) do
            [{ name: 'test' }]
          end

          let(:spec) do
            { :documents     => documents,
              :db_name       => TEST_DB,
              :coll_name     => TEST_COLL,
              :write_concern => Mongo::WriteConcern::Mode.get(:w => 1)
            }
          end

          let(:failing_insert) do
            described_class.new(spec)
          end

          it 'raises an error' do
            expect {
              failing_insert.execute(server.context)
              failing_insert.execute(server.context)
            }.to raise_error(Mongo::Operation::Write::Failure)
          end
        end
      end

      context 'when inserting multiple documents' do

        context 'when the insert succeeds' do

          let(:documents) do
            [{ name: 'test1' }, { name: 'test2' }]
          end

          let(:response) do
            insert.execute(server.context)
          end

          it 'inserts the documents into the database' do
            expect(response.n).to eq(2)
          end
        end

        context 'when the insert fails on the last document' do

          let(:documents) do
            [{ name: 'test3' }, { name: 'test' }]
          end

          let(:spec) do
            { :documents     => documents,
              :db_name       => TEST_DB,
              :coll_name     => TEST_COLL,
              :write_concern => Mongo::WriteConcern::Mode.get(:w => 1)
            }
          end

          let(:failing_insert) do
            described_class.new(spec)
          end

          it 'raises an error' do
            expect {
              failing_insert.execute(server.context)
              failing_insert.execute(server.context)
            }.to raise_error(Mongo::Operation::Write::Failure)
          end
        end

        context 'when the insert fails on the first document' do

          let(:documents) do
            [{ name: 'test' }, { name: 'test4' }]
          end

          let(:spec) do
            { :documents     => documents,
              :db_name       => TEST_DB,
              :coll_name     => TEST_COLL,
              :write_concern => Mongo::WriteConcern::Mode.get(:w => 1)
            }
          end

          let(:failing_insert) do
            described_class.new(spec)
          end

          it 'raises an error' do
            expect {
              failing_insert.execute(server.context)
              failing_insert.execute(server.context)
            }.to raise_error(Mongo::Operation::Write::Failure)
          end
        end
      end
    end

    pending 'when the server is a secondary'
  end
end
