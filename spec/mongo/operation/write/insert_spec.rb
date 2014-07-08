require 'spec_helper'

describe Mongo::Operation::Write::Insert do
  include_context 'operation'

  let(:documents) { [{ :foo => 1 }] }
  let(:spec) do
    { :documents     => documents,
      :db_name       => db_name,
      :coll_name     => coll_name,
      :write_concern => write_concern,
      :ordered       => true
    }
  end

  let(:insert_write_cmd) do
    double('insert_write_cmd').tap do |i|
      allow(i).to receive(:execute) { [] }
    end
  end

  let(:context) { {} }
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
        let(:other_docs) { [{ :bar => 1 }] }
        let(:other_spec) do
          { :documents     => other_docs,
            :db_name       => 'test',
            :coll_name     => 'test_coll',
            :write_concern => { 'w' => 1 },
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
        expect(copy.spec[:documents]).not_to be(op.spec[:documents])
      end
    end
  end

  describe '#merge' do

    context 'same collection and database' do
      let(:other_docs) { [{ :bar => 1 }] }
      let(:other_spec) do
        { :documents     => other_docs,
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
      let(:other_docs) { [{ :bar => 1 }] }
      let(:other_spec) do
        { :documents     => other_docs,
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
      let(:other_docs) { [{ :bar => 1 }] }
      let(:other_spec) do
        { :documents     => other_docs,
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

    context 'merged list of documents' do
      let(:other_docs) { [{ :bar => 1 }] }
      let(:other_spec) do
        { :documents     => other_docs,
          :db_name       => db_name,
          :coll_name     => coll_name
        }
      end
      let(:other) { described_class.new(other_spec) }
      let(:expected) { documents << other_docs }

      it 'merges the list of documents' do
        expect(op.merge(other).spec[:documents]).to eq(expected)
      end
    end

    context 'mutability' do
      let(:other_docs) { [{ :bar => 1 }] }
      let(:other_spec) do
        { :documents     => other_docs,
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
      let(:other_docs) { [{ :bar => 1 }] }
      let(:other_spec) do
        { :documents     => other_docs,
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
      let(:other_docs) { [{ :bar => 1 }] }
      let(:other_spec) do
        { :documents     => other_docs,
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
      let(:other_docs) { [{ :bar => 1 }] }
      let(:other_spec) do
        { :documents     => other_docs,
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

    context 'merged list of documents' do
      let(:other_docs) { [{ :bar => 1 }] }
      let(:other_spec) do
        { :documents     => other_docs,
          :db_name       => db_name,
          :coll_name     => coll_name
        }
      end
      let(:other) { described_class.new(other_spec) }
      let(:expected) { documents << other_docs }

      it 'merges the list of documents' do
        expect(op.merge!(other).spec[:documents]).to eq(expected)
      end
    end

    context 'mutability' do
      let(:other_docs) { [{ :bar => 1 }] }
      let(:other_spec) do
        { :documents     => other_docs,
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

  describe '#split' do

    context 'number of inserts is evenly divisble by divisor' do
      let(:documents) do
        [ { :a => 1 },
          { :b => 1 },
          { :c => 1 },
          { :d => 1 },
          { :e => 1 },
          { :f => 1 } ]
      end
      let(:divisor) { 3 }

      it 'splits the op into the divisor number of children ops' do
        expect(op.split(divisor).size).to eq(divisor)
      end

      it 'divides the inserts evenly between children ops' do
        ops = op.split(divisor)
        batch_size = documents.size / divisor

        divisor.times do |i|
          start_index = i * batch_size
          expect(ops[i].spec[:documents]).to eq(documents[start_index, batch_size])
        end
      end
    end

    context 'number of inserts is not evenly divisble by divisor' do
      let(:documents) do
        [ { :a => 1 },
          { :b => 1 },
          { :c => 1 },
          { :d => 1 },
          { :e => 1 },
          { :f => 1 } ]
      end
      let(:divisor) { 4 }

      it 'splits the op into the divisor number of children ops' do
        expect(op.split(divisor).size).to eq(divisor)
      end

      it 'divides the inserts evenly between children ops' do
        ops = op.split(divisor)
        batch_size = documents.size / divisor

        divisor.times do |i|
          start_index = i * batch_size
          if i == divisor - 1
            expect(ops[i].spec[:documents]).to eq(documents[start_index..-1])
          else
            expect(ops[i].spec[:documents]).to eq(documents[start_index, batch_size])
          end
        end
      end
    end
  end

  describe '#execute' do

    context 'server' do

      context 'when the type is secondary' do

        it 'throws an error' do
          expect{ op.execute(secondary_context) }.to raise_exception
        end
      end

      context 'server has wire version >= 2' do

        it 'creates a write command insert operation' do
          expect(Mongo::Operation::Write::WriteCommand::Insert).to receive(:new) do |sp|
            expect(sp).to eq(spec)
          end.and_return(insert_write_cmd)

          op.execute(primary_context)
        end

        it 'executes the write command insert operation' do
          allow(Mongo::Operation::Write::WriteCommand::Insert).to receive(:new) do
            insert_write_cmd
          end
          expect(insert_write_cmd).to receive(:execute) { [] }
          op.execute(primary_context)
        end
      end

      context 'server has wire version < 2' do

        context 'write concern' do

          context 'w > 0' do

            it 'calls get last error after each message' do
              expect(connection).to receive(:dispatch) do |messages|
                expect(messages.length).to eq(2)
              end
              op.execute(primary_context_2_4_version)
            end
          end

          context 'w == 0' do
            let(:write_concern) { Mongo::WriteConcern::Mode.get(:w => 0) }

            it 'does not call get last error after each message' do
              expect(connection).to receive(:dispatch) do |messages|
                expect(messages.length).to eq(1)
              end
              op.execute(primary_context_2_4_version)
            end
          end
        end

        context 'insert messages' do
          let(:documents) do
            [{ :foo => 1 },
             { :bar => 1 }]
          end

          it 'sends each insert message separately' do
            allow(Mongo::Operation::Write::WriteCommand::Insert).to receive(:new) do
              insert_write_cmd
            end
            expect(connection).to receive(:dispatch).exactly(documents.length)
            op.execute(primary_context_2_4_version)
          end
        end
      end
    end
  end
end
