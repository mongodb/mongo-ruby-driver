# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Cluster::CursorReaper do

  let(:cluster) { double('cluster') }

  before do
    authorized_collection.drop
  end

  let(:reaper) do
    described_class.new(cluster)
  end

  let(:active_cursor_ids) do
    reaper.instance_variable_get(:@active_cursor_ids)
  end

  describe '#intialize' do

    it 'initializes a hash for servers and their kill cursors ops' do
      expect(reaper.instance_variable_get(:@to_kill)).to be_a(Hash)
    end

    it 'initializes a set for the list of active cursors' do
      expect(reaper.instance_variable_get(:@active_cursor_ids)).to be_a(Set)
    end
  end

  describe '#schedule_kill_cursor' do

    let(:address) { Mongo::Address.new('localhost') }
    let(:server) do
      double('server').tap do |server|
        allow(server).to receive(:address).and_return(address)
      end
    end
    let(:session) do
      double(Mongo::Session)
    end
    let(:cursor_id) { 1 }
    let(:cursor_kill_spec_1) do
      Mongo::Cursor::KillSpec.new(
        cursor_id: cursor_id,
        coll_name: 'c',
        db_name: 'd',
        server_address: address,
        connection_global_id: 1,
        session: session,
      )
    end
    let(:cursor_kill_spec_2) do
      Mongo::Cursor::KillSpec.new(
        cursor_id: cursor_id,
        coll_name: 'c',
        db_name: 'q',
        server_address: address,
        connection_global_id: 1,
        session: session,
      )
    end
    let(:to_kill) { reaper.instance_variable_get(:@to_kill)}

    context 'when the cursor is on the list of active cursors' do

      before do
        reaper.register_cursor(cursor_id)
      end

      context 'when there is not a list already for the server' do

        before do
          reaper.schedule_kill_cursor(cursor_kill_spec_1)
          reaper.read_scheduled_kill_specs
        end

        it 'initializes the list of op specs to a set' do
          expect(to_kill.keys).to eq([ address ])
          expect(to_kill[address]).to contain_exactly(cursor_kill_spec_1)
        end
      end

      context 'when there is a list of ops already for the server' do

        before do
          reaper.schedule_kill_cursor(cursor_kill_spec_1)
          reaper.read_scheduled_kill_specs
          reaper.schedule_kill_cursor(cursor_kill_spec_2)
          reaper.read_scheduled_kill_specs
        end

        it 'adds the op to the server list' do
          expect(to_kill.keys).to eq([ address ])
          expect(to_kill[address]).to contain_exactly(cursor_kill_spec_1, cursor_kill_spec_2)
        end

        context 'when the same op is added more than once' do

          before do
            reaper.schedule_kill_cursor(cursor_kill_spec_2)
            reaper.read_scheduled_kill_specs
          end

          it 'does not allow duplicates ops for a server' do
            expect(to_kill.keys).to eq([ address ])
            expect(to_kill[address]).to contain_exactly(cursor_kill_spec_1, cursor_kill_spec_2)
          end
        end
      end
    end

    context 'when the cursor is not on the list of active cursors' do

      before do
        reaper.schedule_kill_cursor(cursor_kill_spec_1)
      end

      it 'does not add the kill cursors op spec to the list' do
        expect(to_kill).to eq({})
      end
    end
  end

  describe '#register_cursor' do

    context 'when the cursor id is nil' do

      let(:cursor_id) do
        nil
      end

      it 'raises exception' do
        expect do
          reaper.register_cursor(cursor_id)
        end.to raise_error(ArgumentError, /register_cursor called with nil cursor_id/)
      end
    end

    context 'when the cursor id is 0' do

      let(:cursor_id) do
        0
      end

      it 'raises exception' do
        expect do
          reaper.register_cursor(cursor_id)
        end.to raise_error(ArgumentError, /register_cursor called with cursor_id=0/)
      end
    end

    context 'when the cursor id is a valid id' do

      let(:cursor_id) do
        2
      end

      before do
        reaper.register_cursor(cursor_id)
      end

      it 'registers the cursor id as active' do
        expect(active_cursor_ids).to eq(Set.new([2]))
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
        expect(active_cursor_ids.size).to eq(0)
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
      cluster.schedule_kill_cursor(
        cursor.kill_spec(
          cursor.instance_variable_get(:@server)
        )
      )
      periodic_executor.flush
      example.run
      periodic_executor.run!
    end

    it 'schedules the kill cursor op' do
      expect {
        cursor.to_a
        # Mongo::Error::SessionEnded is raised here because the periodic executor
        # called in around block kills the cursor and closes the session.
        # This code is normally scheduled in cursor finalizer, so the cursor object
        # is garbage collected when the code is executed. So, a user won't get
        # this exception.
      }.to raise_exception(Mongo::Error::SessionEnded)
    end
  end
end
