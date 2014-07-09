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

  describe '#execute' do

    context 'server' do

      context 'when the type is secondary' do

        it 'throws an error' do
          expect{ op.execute(secondary_context) }.to raise_exception
        end
      end

      context 'server has wire version >= 2' do

        it 'creates a write command delete operation' do
          expect(Mongo::Operation::Write::WriteCommand::Delete).to receive(:new) do |sp|
            expect(sp).to eq(spec)
          end.and_return(delete_write_cmd)

          op.execute(primary_context)
        end

        it 'executes the write command delete operation' do
          allow(Mongo::Operation::Write::WriteCommand::Delete).to receive(:new) do
            delete_write_cmd
          end
          expect(delete_write_cmd).to receive(:execute) { [] }
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

        context 'delete messages' do
          let(:deletes) do
            [{:q => { :foo => 1 }, :limit => 1},
             {:q => { :bar => 1 }, :limit => 1}]
          end

          it 'sends each insert message separately' do
            allow(Mongo::Operation::Write::WriteCommand::Delete).to receive(:new) do
              delete_write_cmd
            end
            expect(connection).to receive(:dispatch).exactly(deletes.length)
            op.execute(primary_context_2_4_version)
          end
        end
      end
    end
  end
end
