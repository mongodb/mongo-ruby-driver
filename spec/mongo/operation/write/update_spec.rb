require 'spec_helper'

describe Mongo::Operation::Write::Update do
  include_context 'operation'

  let(:updates) do
    [{ :q => { :foo => 1 },
       :u => { :$set => { :bar => 1 } },
       :multi => true,
       :upsert => false }]
  end

  let(:spec) do
    { :updates       => updates,
      :db_name       => db_name,
      :coll_name     => coll_name,
      :write_concern => write_concern,
      :ordered       => true
    }
  end

  let(:update_write_cmd) do
    double('update_write_cmd').tap do |u|
      allow(u).to receive(:execute) { [] }
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
        let(:other_updates) { [{:q => { :foo => 1 },
                                :u => { :$set => { :bar => 1 } },
                                :multi => true,
                                :upsert => true }] }
        let(:other_spec) do
          { :updates       => other_updates,
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
        expect(copy.spec[:updates]).not_to be(op.spec[:updates])
      end
    end
  end

  describe '#merge' do

    context 'same collection and database' do
      let(:other_updates) { [{:q => { :foo => 1 },
                              :u => { :$set => { :bar => 1 } },
                              :multi => true,
                              :upsert => true }] }
      let(:other_spec) do
        { :updates       => other_updates,
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
      let(:other_updates) { [{:q => { :foo => 1 },
                              :u => { :$set => { :bar => 1 } },
                              :multi => true,
                              :upsert => true }] }
      let(:other_spec) do
        { :updates       => other_updates,
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
      let(:other_updates) { [{:q => { :foo => 1 },
                              :u => { :$set => { :bar => 1 } },
                              :multi => true,
                              :upsert => true }] }
      let(:other_spec) do
        { :updates       => other_updates,
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
          :db_name       => db_name,
          :coll_name     => coll_name
        }
      end
      let(:other) { described_class.new(other_spec) }
      let(:expected) { updates << other_updates }

      it 'merges the list of deletes' do
        expect(op.merge(other).spec[:updates]).to eq(expected)
      end
    end

    context 'mutability' do
      let(:other_updates) { [{:q => { :foo => 1 },
                              :u => { :$set => { :bar => 1 } },
                              :multi => true,
                              :upsert => true }] }
      let(:other_spec) do
        { :updates       => other_updates,
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
      let(:other_updates) { [{:q => { :foo => 1 },
                              :u => { :$set => { :bar => 1 } },
                              :multi => true,
                              :upsert => true }] }
      let(:other_spec) do
        { :updates       => other_updates,
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
      let(:other_updates) { [{:q => { :foo => 1 },
                              :u => { :$set => { :bar => 1 } },
                              :multi => true,
                              :upsert => true }] }
      let(:other_spec) do
        { :updates       => other_updates,
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
      let(:other_updates) { [{:q => { :foo => 1 },
                              :u => { :$set => { :bar => 1 } },
                              :multi => true,
                              :upsert => true }] }
      let(:other_spec) do
        { :updates       => other_updates,
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
      let(:other) { Mongo::Write::Insert.new(spec) }

      it 'raises an exception' do
        expect{ op.merge!(other) }.to raise_exception
      end
    end

    context 'merged updates' do
      let(:other_updates) { [{:q => { :foo => 1 },
                              :u => { :$set => { :bar => 1 } },
                              :multi => true,
                              :upsert => true }] }
      let(:other_spec) do
        { :updates       => other_updates,
          :db_name       => db_name,
          :coll_name     => coll_name
        }
      end
      let(:other) { described_class.new(other_spec) }
      let(:expected) { updates << other_updates }

      it 'merges the list of deletes' do
        expect(op.merge!(other).spec[:updates]).to eq(expected)
      end
    end

    context 'mutability' do
      let(:other_updates) { [{:q => { :foo => 1 },
                              :u => { :$set => { :bar => 1 } },
                              :multi => true,
                              :upsert => true }] }
      let(:other_spec) do
        { :updates       => other_updates,
          :db_name       => db_name,
          :coll_name     => coll_name
        }
      end
      let(:other) { described_class.new(other_spec) }

      it 'mutates the operation itself' do
        expect(op.merge!(other)).to be(op)
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

        it 'creates a write command update operation' do
          expect(Mongo::Operation::Write::WriteCommand::Update).to receive(:new) do |sp|
            expect(sp).to eq(spec)
          end.and_return(update_write_cmd)

          op.execute(primary_context)
        end

        it 'executes the write command update operation' do
          allow(Mongo::Operation::Write::WriteCommand::Update).to receive(:new) do
            update_write_cmd
          end
          expect(update_write_cmd).to receive(:execute) { [] }
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

        context 'update messages' do
          let(:updates) do
            [{ :q => { :foo => 1 },
               :u => { :$set => { :bar => 1 } },
               :multi => true,
               :upsert => false },
             { :q => { :foo => 2 },
               :u => { :$set => { :bar => 2 } },
               :multi => true,
               :upsert => false }]
          end

          it 'sends each update message separately' do
            allow(Mongo::Operation::Write::WriteCommand::Update).to receive(:new) do
              update_write_cmd
            end
            expect(connection).to receive(:dispatch).exactly(updates.length)
            op.execute(primary_context_2_4_version)
          end
        end
      end
    end
  end
end
