require 'spec_helper'

describe Mongo::Cluster::CursorReaper do

  after do
    authorized_collection.delete_many
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

  describe '#run' do

    it 'starts a thread calling #kill_cursors' do
      reaper.run!
      expect(reaper.instance_variable_get(:@thread)).to be_a(Thread)
    end

    context 'when run is called more than once' do

      let!(:reaper_thread) do
        reaper.run!
        reaper.instance_variable_get(:@reaper)
      end

      it 'only starts a thread once' do
        reaper.run!
        expect(reaper.instance_variable_get(:@reaper)).to be(reaper_thread)
      end
    end

    context 'when there are ops in the list to execute' do

      let(:server) { double('server') }
      let(:cursor_id) { 1 }
      let(:op_spec_1) { double('op_spec_1') }
      let(:op_spec_2) { double('op_spec_2') }
      let(:to_kill) { reaper.instance_variable_get(:@to_kill)}

      before do
        reaper.register_cursor(cursor_id)
        reaper.schedule_kill_cursor(cursor_id, op_spec_1, server)
        reaper.run!
        sleep(Mongo::Cluster::CursorReaper::FREQUENCY + 1)
      end

      it 'executes the ops in the thread' do
        expect(reaper.instance_variable_get(:@to_kill).size).to eq(0)
      end
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

  describe '#stop!' do

    let(:thread) do
      reaper.run!
      reaper.stop!
      sleep(0.5)
      reaper.instance_variable_get(:@thread)
    end

    it 'stops the thread from running' do
      expect(thread.alive?).to be(false)
    end
  end

  describe '#restart!' do

    let(:thread) do
      reaper.run!
      reaper.stop!
      sleep(0.5)
      reaper.restart!
      reaper.instance_variable_get(:@thread)
    end

    it 'restarts the thread' do
      expect(thread.alive?).to be(true)
    end
  end
end
