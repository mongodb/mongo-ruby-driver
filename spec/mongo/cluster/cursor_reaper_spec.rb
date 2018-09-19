require 'spec_helper'

describe Mongo::Cluster::CursorReaper do

  before do
    authorized_collection.drop
  end

  let(:reaper) do
    described_class.new
  end

  let(:active_cursors) do
    reaper.instance_variable_get(:@active_cursors)
  end

  describe '#intialize' do

    it 'initializes a hash for servers and their kill cursors ops' do
      expect(reaper.instance_variable_get(:@to_kill)).to be_a(Hash)
    end

    it 'initializes a set for the list of active cursors' do
      expect(reaper.instance_variable_get(:@active_cursors)).to be_a(Set)
    end
  end

  describe '#schedule_kill_cursor' do

    let(:server) { double('server') }
    let(:cursor_id) { 1 }
    let(:op_spec_1) { double('op_spec_1') }
    let(:op_spec_2) { double('op_spec_2') }
    let(:to_kill) { reaper.instance_variable_get(:@to_kill)}

    context 'when the cursor is on the list of active cursors' do

      before do
        reaper.register_cursor(cursor_id)
      end

      context 'when there is not a list already for the server' do

        before do
          reaper.schedule_kill_cursor(cursor_id, op_spec_1, server)
        end

        it 'initializes the list of op specs to a set' do
          expect(to_kill.keys).to eq([ server ])
          expect(to_kill[server]).to eq(Set.new([op_spec_1]))
        end
      end

      context 'when there is a list of ops already for the server' do

        before do
          reaper.schedule_kill_cursor(cursor_id, op_spec_1, server)
          reaper.schedule_kill_cursor(cursor_id, op_spec_2, server)
        end

        it 'adds the op to the server list' do
          expect(to_kill.keys).to eq([ server ])
          expect(to_kill[server]).to contain_exactly(op_spec_1, op_spec_2)
        end

        context 'when the same op is added more than once' do

          before do
            reaper.schedule_kill_cursor(cursor_id, op_spec_2, server)
          end

          it 'does not allow duplicates ops for a server' do
            expect(to_kill.keys).to eq([ server ])
            expect(to_kill[server]).to contain_exactly(op_spec_1, op_spec_2)
          end
        end
      end
    end

    context 'when the cursor is not on the list of active cursors' do

      before do
        reaper.schedule_kill_cursor(cursor_id, op_spec_1, server)
      end

      it 'does not add the kill cursors op spec to the list' do
        expect(to_kill).to eq({})
      end
    end
  end

  describe '#register_cursor' do

    before do
      reaper.register_cursor(cursor_id)
    end

    context 'when the cursor id is nil' do

      let(:cursor_id) do
        nil
      end

      it 'does not register the cursor' do
        expect(active_cursors.size).to be(0)
      end
    end

    context 'when the cursor id is 0' do

      let(:cursor_id) do
        0
      end

      it 'does not register the cursor' do
        expect(active_cursors.size).to be(0)
      end
    end

    context 'when the cursor id is a valid id' do

      let(:cursor_id) do
        2
      end

      it 'registers the cursor id as active' do
        expect(active_cursors).to eq(Set.new([2]))
      end
    end
  end

  describe '#unregister_cursor' do

    context 'when the cursor id is in the active cursors list' do

      before do
        reaper.register_cursor(2)
        reaper.unregister_cursor(2)
      end

      it 'removes the cursor id' do
        expect(active_cursors.size).to eq(0)
      end
    end
  end

  context 'when a non-exhausted cursor goes out of scope' do

    let(:docs) do
      103.times.collect { |i| { a: i } }
    end

    let(:periodic_executor) do
      cluster.instance_variable_get(:@periodic_executor)
    end

    let(:cluster) do
      authorized_client.cluster
    end

    let(:cursor) do
      view = authorized_collection.find
      view.to_enum.next
      cursor = view.instance_variable_get(:@cursor)
    end

    around do |example|
      authorized_collection.insert_many(docs)
      periodic_executor.stop!
      cluster.schedule_kill_cursor(cursor.id, cursor.send(:kill_cursors_op_spec),
                                   cursor.instance_variable_get(:@server))
      periodic_executor.flush
      example.run
      periodic_executor.run!
    end

    it 'schedules the kill cursor op' do
      expect {
        cursor.to_a
      }.to raise_exception(Mongo::Error::OperationFailure)
    end
  end
end
