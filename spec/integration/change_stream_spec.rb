require 'spec_helper'

describe 'Change stream integration' do
  only_mri
  max_example_run_time 7

  FAIL_POINT_BASE_COMMAND = { 'configureFailPoint' => "failCommand" }

  before do
    unless test_change_streams?
      skip 'Not testing change streams'
    end
  end

  describe 'watch+next' do
    shared_context 'returns a change document' do
      it 'returns a change document' do
        cs = authorized_collection.watch

        authorized_collection.insert_one(:a => 1)
        sleep 0.5

        change = cs.to_enum.next
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
        cs = authorized_collection.watch

        authorized_collection.insert_one(:a => 1)

        change = cs.to_enum.next
        expect(change).to be_a(BSON::Document)
        expect(change['operationType']).to eql('insert')
        doc = change['fullDocument']
        expect(doc['_id']).to be_a(BSON::ObjectId)
        doc.delete('_id')
        expect(doc).to eql('a' => 1)
      end
    end

    context 'error on initial aggregation' do
      min_server_version '4.0'

      before do
        authorized_collection.client.use(:admin).command(FAIL_POINT_BASE_COMMAND.merge(
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
      min_server_version '4.0'

      before do
        authorized_collection.client.use(:admin).command(FAIL_POINT_BASE_COMMAND.merge(
          :mode => {:times => 1},
          :data => {:failCommands => ['getMore'], errorCode: 100}))
      end

      it_behaves_like 'returns a change document'
    end

    context 'two errors on getMore' do
      min_server_version '4.0'

      before do
        authorized_collection.client.use(:admin).command(FAIL_POINT_BASE_COMMAND.merge(
          :mode => {:times => 2},
          :data => {:failCommands => ['getMore'], errorCode: 100}))
      end

      # this retries twice because aggregation resets retry count,
      # and ultimately succeeds and returns data
      it_behaves_like 'returns a change document'
    end

    context 'two errors on getMore followed by an error on aggregation' do
      min_server_version '4.0'

      it 'next raises error' do
        cs = authorized_collection.watch

        authorized_collection.insert_one(:a => 1)
        sleep 0.5

        enum = cs.to_enum

        authorized_collection.client.use(:admin).command(FAIL_POINT_BASE_COMMAND.merge(
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
    shared_context 'returns a change document' do
      it 'returns a change document' do
        cs = authorized_collection.watch

        authorized_collection.insert_one(:a => 1)
        sleep 0.5

        change = cs.to_enum.try_next
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
        cs = authorized_collection.watch

        change = cs.to_enum.try_next
        expect(change).to be nil
      end
    end

    context 'one error on getMore' do
      min_server_version '4.0'

      before do
        authorized_collection.client.use(:admin).command(FAIL_POINT_BASE_COMMAND.merge(
          :mode => {:times => 1},
          :data => {:failCommands => ['getMore'], errorCode: 100}))
      end

      it_behaves_like 'returns a change document'
    end

    context 'two errors on getMore' do
      min_server_version '4.0'

      before do
        authorized_collection.client.use(:admin).command(FAIL_POINT_BASE_COMMAND.merge(
          :mode => {:times => 2},
          :data => {:failCommands => ['getMore'], errorCode: 100}))
      end

      # this retries twice because aggregation resets retry count,
      # and ultimately succeeds and returns data
      it_behaves_like 'returns a change document'
    end

    context 'two errors on getMore followed by an error on aggregation' do
      min_server_version '4.0'

      it 'next raises error' do
        cs = authorized_collection.watch

        authorized_collection.insert_one(:a => 1)
        sleep 0.5

        enum = cs.to_enum

        authorized_collection.client.use(:admin).command(FAIL_POINT_BASE_COMMAND.merge(
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
