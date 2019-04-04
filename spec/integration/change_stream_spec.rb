require 'spec_helper'

describe 'Change stream integration', retry: 4 do
  only_mri
  max_example_run_time 7
  min_server_fcv '3.6'
  require_topology :replica_set

  let(:fail_point_base_command) do
    { 'configureFailPoint' => "failCommand" }
  end

  # There is value in not clearing fail points between tests because
  # their triggering will distinguish fail points not being set vs
  # them not being triggered
  def clear_fail_point(collection)
    collection.client.use(:admin).command(fail_point_base_command.merge(mode: "off"))
  end

  class << self
    def clear_fail_point_before
      before do
        clear_fail_point(authorized_collection)
      end
    end
  end

  describe 'watch+next' do
    let(:change_stream) { authorized_collection.watch }

    shared_context 'returns a change document' do
      it 'returns a change document' do
        change_stream

        authorized_collection.insert_one(:a => 1)
        sleep 0.5

        change = change_stream.to_enum.next
        expect(change).to be_a(BSON::Document)
        expect(change['operationType']).to eql('insert')
        doc = change['fullDocument']
        expect(doc['_id']).to be_a(BSON::ObjectId)
        doc.delete('_id')
        expect(doc).to eql('a' => 1)
      end
    end

    context 'no errors' do
      it 'next returns changes' do
        change_stream

        authorized_collection.insert_one(:a => 1)

        change = change_stream.to_enum.next
        expect(change).to be_a(BSON::Document)
        expect(change['operationType']).to eql('insert')
        doc = change['fullDocument']
        expect(doc['_id']).to be_a(BSON::ObjectId)
        doc.delete('_id')
        expect(doc).to eql('a' => 1)
      end
    end

    context 'error on initial aggregation' do
      min_server_fcv '4.0'
      clear_fail_point_before

      before do
        authorized_collection.client.use(:admin).command(fail_point_base_command.merge(
          :mode => {:times => 1},
          :data => {:failCommands => ['aggregate'], errorCode: 100}))
      end

      it 'watch raises error' do
        expect do
          authorized_collection.watch
        end.to raise_error(Mongo::Error::OperationFailure, "Failing command due to 'failCommand' failpoint (100)")
      end
    end

    context 'one error on getMore' do
      min_server_fcv '4.0'
      clear_fail_point_before

      context 'error on first getMore' do
        before do
          authorized_collection.client.use(:admin).command(fail_point_base_command.merge(
            :mode => {:times => 1},
            :data => {:failCommands => ['getMore'], errorCode: 100}))
        end

        it_behaves_like 'returns a change document'
      end

      context 'error on a getMore other than first' do
        before do
          # Need to retrieve a change stream document successfully prior to
          # failing to have the resume token, otherwise the change stream
          # ignores documents inserted after the first aggregation
          # and the test gets stuck
          change_stream
          authorized_collection.insert_one(:a => 1)
          change_stream.to_enum.next
          authorized_collection.insert_one(:a => 1)

          authorized_collection.client.use(:admin).command(fail_point_base_command.merge(
            :mode => {:times => 1},
            :data => {:failCommands => ['getMore'], errorCode: 100}))
        end

        it_behaves_like 'returns a change document'
      end
    end

    context 'two errors on getMore' do
      min_server_fcv '4.0'
      clear_fail_point_before

      context 'error of first getMores' do
        before do
          authorized_collection.client.use(:admin).command(fail_point_base_command.merge(
            :mode => {:times => 2},
            :data => {:failCommands => ['getMore'], errorCode: 100}))
        end

        # this retries twice because aggregation resets retry count,
        # and ultimately succeeds and returns data
        it_behaves_like 'returns a change document'
      end

      context 'error on a getMore other than first' do
        before do
          # Need to retrieve a change stream document successfully prior to
          # failing to have the resume token, otherwise the change stream
          # ignores documents inserted after the first aggregation
          # and the test gets stuck
          change_stream
          authorized_collection.insert_one(:a => 1)
          change_stream.to_enum.next
          authorized_collection.insert_one(:a => 1)

          authorized_collection.client.use(:admin).command(fail_point_base_command.merge(
            :mode => {:times => 2},
            :data => {:failCommands => ['getMore'], errorCode: 100}))
        end

        # this retries twice because aggregation resets retry count,
        # and ultimately succeeds and returns data
        it_behaves_like 'returns a change document'
      end
    end

    context 'two errors on getMore followed by an error on aggregation' do
      min_server_fcv '4.0'
      clear_fail_point_before

      it 'next raises error' do
        change_stream

        sleep 0.5
        authorized_collection.insert_one(:a => 1)
        sleep 0.5

        enum = change_stream.to_enum

        authorized_collection.client.use(:admin).command(fail_point_base_command.merge(
          :mode => {:times => 2},
          :data => {:failCommands => ['getMore', 'aggregate'], errorCode: 101}))

        sleep 0.5

        expect do
          enum.next
        end.to raise_error(Mongo::Error::OperationFailure, "Failing command due to 'failCommand' failpoint (101)")
      end
    end
  end

  describe 'try_next' do
    let(:change_stream) { authorized_collection.watch }

    shared_context 'returns a change document' do
      it 'returns a change document' do
        change_stream

        sleep 0.5
        authorized_collection.insert_one(:a => 1)
        sleep 0.5

        change = change_stream.to_enum.try_next
        expect(change).to be_a(BSON::Document)
        expect(change['operationType']).to eql('insert')
        doc = change['fullDocument']
        expect(doc['_id']).to be_a(BSON::ObjectId)
        doc.delete('_id')
        expect(doc).to eql('a' => 1)
      end
    end

    context 'there are changes' do
      it_behaves_like 'returns a change document'
    end

    context 'there are no changes' do
      it 'returns nil' do
        change_stream

        change = change_stream.to_enum.try_next
        expect(change).to be nil
      end
    end

    context 'one error on getMore' do
      min_server_fcv '4.0'
      clear_fail_point_before

      context 'error on first getMore' do
        before do
          authorized_collection.client.use(:admin).command(fail_point_base_command.merge(
            :mode => {:times => 1},
            :data => {:failCommands => ['getMore'], errorCode: 100}))
        end

        it_behaves_like 'returns a change document'
      end

      context 'error on a getMore other than first' do
        before do
          change_stream
          authorized_collection.insert_one(:a => 1)
          change_stream.to_enum.next
          authorized_collection.insert_one(:a => 1)

          authorized_collection.client.use(:admin).command(fail_point_base_command.merge(
            :mode => {:times => 1},
            :data => {:failCommands => ['getMore'], errorCode: 100}))
        end

        it_behaves_like 'returns a change document'
      end
    end

    context 'two errors on getMore' do
      min_server_fcv '4.0'
      clear_fail_point_before

      before do
        # Note: this fail point seems to be broken in 4.0 < 4.0.5
        # (command to set it returns success but the fail point is not set).
        # The test succeeds in this case but doesn't test two errors on
        # getMore as no errors actually happen.
        # 4.0.5-dev server appears to correctly set the fail point.
        authorized_collection.client.use(:admin).command(fail_point_base_command.merge(
          :mode => {:times => 2},
          :data => {:failCommands => ['getMore'], errorCode: 100}))
      end

      # this retries twice because aggregation resets retry count,
      # and ultimately succeeds and returns data
      it_behaves_like 'returns a change document'
    end

    context 'two errors on getMore followed by an error on aggregation' do
      min_server_fcv '4.0'
      clear_fail_point_before

      context 'error on first getMore' do
        it 'next raises error' do
          change_stream

          sleep 0.5
          authorized_collection.insert_one(:a => 1)
          sleep 0.5

          enum = change_stream.to_enum

          authorized_collection.client.use(:admin).command(fail_point_base_command.merge(
            :mode => {:times => 3},
            :data => {:failCommands => ['getMore', 'aggregate'], errorCode: 101}))

          sleep 0.5

          expect do
            enum.try_next
          end.to raise_error(Mongo::Error::OperationFailure, "Failing command due to 'failCommand' failpoint (101)")
        end
      end

      context 'error on a getMore other than first' do
        it 'next raises error' do
          change_stream

          authorized_collection.insert_one(:a => 1)
          change_stream.to_enum.next
          authorized_collection.insert_one(:a => 1)
          sleep 0.5

          enum = change_stream.to_enum

          authorized_collection.client.use(:admin).command(fail_point_base_command.merge(
            :mode => {:times => 3},
            :data => {:failCommands => ['getMore', 'aggregate'], errorCode: 101}))

          sleep 0.5

          expect do
            enum.try_next
          end.to raise_error(Mongo::Error::OperationFailure, "Failing command due to 'failCommand' failpoint (101)")
        end
      end
    end
  end

  describe ':start_at_operation_time option' do
    min_server_fcv '4.0'

    before do
      authorized_collection.delete_many
    end

    it 'respects start time prior to beginning of aggregation' do
      time = Time.now - 1
      authorized_collection.insert_one(:a => 1)
      sleep 0.5

      cs = authorized_collection.watch([], start_at_operation_time: time)

      document = cs.to_enum.next
      expect(document).to be_a(BSON::Document)
    end

    it 'respects start time after beginning of aggregation' do
      time = Time.now + 10
      cs = authorized_collection.watch([], start_at_operation_time: time)
      sleep 0.5

      authorized_collection.insert_one(:a => 1)

      sleep 0.5

      document = cs.to_enum.try_next
      expect(document).to be_nil
    end

    it 'accepts a Time' do
      time = Time.now
      cs = authorized_collection.watch([], start_at_operation_time: time)
    end

    it 'accepts a BSON::Timestamp' do
      time = BSON::Timestamp.new(Time.now.to_i, 1)
      cs = authorized_collection.watch([], start_at_operation_time: time)
    end

    it 'rejects a Date' do
      time = Date.today
      expect do
        authorized_collection.watch([], start_at_operation_time: time)
      end.to raise_error(ArgumentError, 'Time must be a Time or a BSON::Timestamp instance')
    end

    it 'rejects an integer' do
      time = 1
      expect do
        authorized_collection.watch([], start_at_operation_time: time)
      end.to raise_error(ArgumentError, 'Time must be a Time or a BSON::Timestamp instance')
    end
  end

  describe ':start_after option' do
    require_topology :replica_set
    min_server_version '4.1'

    let(:start_after) do
      stream = authorized_collection.watch([])
      authorized_collection.insert_one(x: 1)
      start_after = stream.to_enum.next['_id']
    end

    let(:stream) do
      authorized_collection.watch([], { start_after: start_after })
    end

    let(:events) do
      start_after

      subscriber = EventSubscriber.new
      authorized_client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
      use_stream

      subscriber.started_events.select { |e| e.command_name == 'aggregate' }
    end

    context 'when an initial aggregation is run' do
      let(:use_stream) do
        stream
      end

      it 'sends startAfter' do
        expect(events.size >= 1).to eq(true)

        command = events.first.command
        expect(command['pipeline'].size == 1).to eq(true)
        expect(command['pipeline'].first.key?('$changeStream')).to eq(true)
        expect(command['pipeline'].first['$changeStream'].key?('startAfter')).to eq(true)
      end
    end

    context 'when resuming' do
      let(:use_stream) do
        stream

        authorized_collection.insert_one(x: 1)
        stream.to_enum.next

        authorized_collection.insert_one(x: 1)
        authorized_collection.client.use(:admin).command(fail_point_base_command.merge(
          :mode => {:times => 1},
          :data => {:failCommands => ['getMore'], errorCode: 100}))
        stream.to_enum.next
      end

      it 'does not startAfter even when passed in' do
        expect(events.size == 2).to eq(true)

        command = events.last.command
        expect(command['pipeline'].size == 1).to eq(true)
        expect(command['pipeline'].first.key?('$changeStream')).to eq(true)
        expect(command['pipeline'].first['$changeStream'].key?('startAfter')).to eq(false)
      end
    end
  end
end
