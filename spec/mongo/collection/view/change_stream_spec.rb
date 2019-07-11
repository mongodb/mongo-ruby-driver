require 'spec_helper'

describe Mongo::Collection::View::ChangeStream do
  require_wired_tiger
  min_server_fcv '3.6'
  require_topology :replica_set
  max_example_run_time 7

  let(:pipeline) do
    []
  end

  let(:options) do
    {}
  end

  let(:view_options) do
    {}
  end

  let(:client) do
    authorized_client_without_any_retry_reads
  end

  let(:collection) do
    client['mcv-change-stream']
  end

  let(:view) do
    Mongo::Collection::View.new(collection, {}, view_options)
  end

  let(:change_stream) do
    @change_stream = described_class.new(view, pipeline, nil, options)
  end

  let(:change_stream_document) do
    change_stream.send(:instance_variable_set, '@resuming', false)
    change_stream.send(:pipeline)[0]['$changeStream']
  end

  let!(:sample_resume_token) do
    stream = collection.watch
    collection.insert_one(a: 1)
    doc = stream.to_enum.next
    stream.close
    doc[:_id]
  end

  let(:command_selector) do
    command_spec[:selector]
  end

  let(:command_spec) do
    change_stream.send(:instance_variable_set, '@resuming', false)
    change_stream.send(:aggregate_spec, double('session'))
  end

  let(:cursor) do
    change_stream.instance_variable_get(:@cursor)
  end

  let(:error) do
    begin
      change_stream
    rescue => e
      e
    else
      nil
    end
  end

  before do
    collection.delete_many
  end

  after do
    # Only close the change stream if one was successfully created by the test
    if @change_stream
      @change_stream.close
    end
  end

  describe '#initialize' do

    it 'sets the view' do
      expect(change_stream.view).to be(view)
    end

    it 'sets the options' do
      expect(change_stream.options).to eq(options)
    end

    context 'when full_document is provided' do

      context "when the value is 'default'" do

        let(:options) do
          { full_document: 'default' }
        end

        it 'sets the fullDocument value to default' do
          expect(change_stream_document[:fullDocument]).to eq('default')
        end
      end

      context "when the value is 'updateLookup'" do

        let(:options) do
          { full_document: 'updateLookup' }
        end

        it 'sets the fullDocument value to updateLookup' do
          expect(change_stream_document[:fullDocument]).to eq('updateLookup')
        end
      end
    end

    context 'when full_document is not provided' do

      it "defaults to use the 'default' value" do
        expect(change_stream_document[:fullDocument]).to eq('default')
      end
    end

    context 'when resume_after is provided' do

      let(:options) do
        { resume_after: sample_resume_token }
      end

      it 'sets the resumeAfter value to the provided document' do
        expect(change_stream_document[:resumeAfter]).to eq(sample_resume_token)
      end
    end

    context 'when max_await_time_ms is provided' do

      let(:options) do
        { max_await_time_ms: 10 }
      end

      it 'sets the maxTimeMS value to the provided document' do
        expect(command_selector[:maxTimeMS]).to eq(10)
      end
    end

    context 'when batch_size is provided' do

      let(:options) do
        { batch_size: 5 }
      end

      it 'sets the batchSize value to the provided document' do
        expect(command_selector[:cursor][:batchSize]).to eq(5)
      end
    end

    context 'when collation is provided'  do

      let(:options) do
        { 'collation' => { locale: 'en_US', strength: 2 } }
      end

      it 'sets the collation value to the provided document' do
        expect(command_selector['collation']).to eq(BSON::Document.new(options['collation']))
      end
    end

    context 'when a changeStream operator is provided by the user as well' do

      let(:pipeline) do
        [ { '$changeStream' => { fullDocument: 'default' } }]
      end

      it 'raises the error from the server' do
        expect(error).to be_a(Mongo::Error::OperationFailure)
        expect(error.message).to include('$changeStream is only valid as the first stage in a pipeline')
      end
    end

    context 'when the collection has a readConcern' do

      let(:collection) do
        client['mcv-change-stream'].with(
          read_concern: { level: 'majority' })
      end

      let(:view) do
        Mongo::Collection::View.new(collection, {}, options)
      end

      it 'uses the read concern of the collection' do
        expect(command_selector[:readConcern]).to eq('level' => 'majority')
      end
    end

    context 'when no pipeline is supplied' do

      it 'uses an empty pipeline' do
        expect(command_selector[:pipeline][0].keys).to eq(['$changeStream'])
      end
    end

    context 'when other pipeline operators are supplied' do

      context 'when the other pipeline operators are supported' do

        let(:pipeline) do
          [{ '$project' => { '_id' => 0 }}]
        end

        it 'uses the pipeline operators' do
          expect(command_selector[:pipeline][1]).to eq(pipeline[0])
        end
      end

      context 'when the other pipeline operators are not supported' do

        let(:pipeline) do
          [{ '$unwind' => '$test' }]
        end

        it 'sends the pipeline to the server without a custom error' do
          expect {
            change_stream
          }.to raise_exception(Mongo::Error::OperationFailure)
        end

        context 'when the operation fails' do

          let!(:before_last_use) do
            session.instance_variable_get(:@server_session).last_use
          end

          let!(:before_operation_time) do
            (session.operation_time || 0)
          end

          let(:pipeline) do
            [ { '$invalid' => '$test' }]
          end

          let(:options) do
            { session: session }
          end

          let!(:operation_result) do
            begin; change_stream; rescue => e; e; end
          end

          let(:session) do
            client.start_session
          end

          it 'raises an error' do
            expect(operation_result.class).to eq(Mongo::Error::OperationFailure)
          end

          it 'updates the last use value' do
            expect(session.instance_variable_get(:@server_session).last_use).not_to eq(before_last_use)
          end

          it 'updates the operation time value' do
            expect(session.operation_time).not_to eq(before_operation_time)
          end
        end
      end
    end

    context 'when the initial batch is empty' do

      before do
        change_stream
      end

      it 'does not close the cursor' do
        expect(cursor).to be_a(Mongo::Cursor)
        expect(cursor.closed?).to be false
      end
    end

    context 'when provided a session' do

      let(:options) do
        { session: session }
      end

      let(:operation) do
        change_stream
        collection.insert_one(a: 1)
        change_stream.to_enum.next
      end

      context 'when the session is created from the same client used for the operation' do

        let(:session) do
          client.start_session
        end

        let(:server_session) do
          session.instance_variable_get(:@server_session)
        end

        let!(:before_last_use) do
          server_session.last_use
        end

        let!(:before_operation_time) do
          (session.operation_time || 0)
        end

        let!(:operation_result) do
          operation
        end

        it 'updates the last use value' do
          expect(server_session.last_use).not_to eq(before_last_use)
        end

        it 'updates the operation time value' do
          expect(session.operation_time).not_to eq(before_operation_time)
        end

        it 'does not close the session when the operation completes' do
          expect(session.ended?).to be(false)
        end
      end

      context 'when a session from another client is provided' do

        let(:session) do
          another_authorized_client.with(retry_reads: false).start_session
        end

        let(:operation_result) do
          operation
        end

        it 'raises an exception' do
          expect {
            operation_result
          }.to raise_exception(Mongo::Error::InvalidSession)
        end
      end

      context 'when the session is ended before it is used' do

        let(:session) do
          client.start_session
        end

        before do
          session.end_session
        end

        let(:operation_result) do
          operation
        end

        it 'raises an exception' do
          expect {
            operation_result
          }.to raise_exception(Mongo::Error::InvalidSession)
        end
      end
    end
  end

  describe '#close' do

    context 'when documents have not been retrieved and the stream is closed' do

      before do
        expect(cursor).to receive(:kill_cursors)
        change_stream.close
      end

      it 'closes the cursor' do
        expect(change_stream.instance_variable_get(:@cursor)).to be(nil)
        expect(change_stream.closed?).to be(true)
      end

      it 'raises an error when the stream is attempted to be iterated' do
        expect {
          change_stream.to_enum.next
        }.to raise_exception(StopIteration)
      end
    end

    context 'when some documents have been retrieved and the stream is closed before sending getMore' do
      fails_on_jruby

      before do
        change_stream
        collection.insert_one(a: 1)
        enum.next
        change_stream.close
      end

      let(:enum) do
        change_stream.to_enum
      end

      it 'raises an error' do
        expect {
          enum.next
        }.to raise_exception(StopIteration)
      end
    end
  end

  describe '#closed?' do

    context 'when the change stream has not been closed' do

      it 'returns false' do
        expect(change_stream.closed?).to be(false)
      end
    end

    context 'when the change stream has been closed' do

      before do
        change_stream.close
      end

      it 'returns false' do
        expect(change_stream.closed?).to be(true)
      end
    end
  end

  context 'when the first response does not contain the resume token' do

    let(:pipeline) do
      # This removes id from change stream document which is used as
      # resume token
      [{ '$project' => { _id: 0 } }]
    end

    before do
      change_stream
      collection.insert_one(a: 1)
    end

    context 'pre-4.2 server' do
      max_server_version '4.0'

      it 'driver raises an exception and closes the cursor' do
        expect(cursor).to receive(:kill_cursors).and_call_original
        expect {
          change_stream.to_enum.next
        }.to raise_exception(Mongo::Error::MissingResumeToken)
      end
    end

    context '4.2+ server' do
      min_server_fcv '4.2'

      it 'server errors, driver closes the cursor' do
        expect(cursor).to receive(:kill_cursors).and_call_original
        expect {
          change_stream.to_enum.next
        }.to raise_exception(Mongo::Error::OperationFailure, /Encountered an event whose _id field, which contains the resume token, was modified by the pipeline. Modifying the _id field of an event makes it impossible to resume the stream from that point. Only transformations that retain the unmodified _id field are allowed./)
      end
    end
  end

  context 'when an error is encountered the first time the command is run' do
    include PrimarySocket

    before do
      expect(primary_socket).to receive(:write).and_raise(error).once
    end

    let(:document) do
      change_stream.to_enum.next
    end

    shared_examples_for 'a resumable change stream' do

      before do
        expect(view.send(:server_selector)).to receive(:select_server).twice.and_call_original
        change_stream
        collection.insert_one(a: 1)
      end

      it 'runs the command again while using the same read preference and caches the resume token' do
        expect(document[:fullDocument][:a]).to eq(1)
        expect(change_stream_document[:resumeAfter]).to eq(document[:_id])
      end

      context 'when provided a session' do

        let(:options) do
          { session: session}
        end

        let(:session) do
          client.start_session
        end

        before do
          change_stream.to_enum.next
        end

        it 'does not close the session' do
          expect(session.ended?).to be(false)
        end
      end
    end

    shared_examples_for 'a non-resumed change stream' do
      it 'does not run the command again and instead raises the error' do
        expect do
          document
        end.to raise_exception(error)
      end
    end

    context 'when the error is a resumable error' do

      context 'when the error is a SocketError' do

        let(:error) do
          Mongo::Error::SocketError
        end

        it_behaves_like 'a non-resumed change stream'
      end

      context 'when the error is a SocketTimeoutError' do

        let(:error) do
          Mongo::Error::SocketTimeoutError
        end

        it_behaves_like 'a non-resumed change stream'
      end

      context "when the error is a 'not master' error" do

        let(:error) do
          Mongo::Error::OperationFailure.new('not master')
        end

        it_behaves_like 'a non-resumed change stream'
      end

      context "when the error is a 'node is recovering' error" do

        let(:error) do
          Mongo::Error::OperationFailure.new('node is recovering')
        end

        it_behaves_like 'a non-resumed change stream'
      end
    end

    context 'when the error is another server error' do

      let(:error) do
        Mongo::Error::MissingResumeToken
      end

      before do
        expect(view.send(:server_selector)).to receive(:select_server).and_call_original
      end

      it_behaves_like 'a non-resumed change stream'

      context 'when provided a session' do

        let(:options) do
          { session: session}
        end

        let(:session) do
          client.start_session
        end

        before do
          expect do
            change_stream
          end.to raise_error(error)
        end

        it 'does not close the session' do
          expect(session.ended?).to be(false)
        end
      end
    end
  end

  context 'when a killCursors command is issued for the cursor' do
    before do
      change_stream
      collection.insert_one(a:1)
      enum.next
      collection.insert_one(a:2)
    end

    let(:enum) do
      change_stream.to_enum
    end

    it 'should create a new cursor and resume' do
      original_cursor_id = cursor.id

      client.use(:admin).command({
        killCursors: collection.name,
        cursors: [cursor.id]
      })

      document = enum.next
      expect(document[:fullDocument][:a]).to eq(2)

      new_cursor_id = change_stream.instance_variable_get(:@cursor).id
      expect(new_cursor_id).not_to eq(original_cursor_id)
    end
  end

  context 'when a server error is encountered during a getMore' do
    fails_on_jruby

    context 'when the error is a resumable error' do

      shared_examples_for 'a change stream that encounters an error from a getMore' do

        before do
          change_stream
          collection.insert_one(a: 1)
          enum.next
          collection.insert_one(a: 2)
          expect(cursor).to receive(:get_more).once.and_raise(error)
          expect(cursor).to receive(:kill_cursors).and_call_original
          expect(view.send(:server_selector)).to receive(:select_server).once.and_call_original
          expect(Mongo::Operation::Aggregate).to receive(:new).and_call_original
        end

        let(:enum) do
          change_stream.to_enum
        end

        let(:document) do
          enum.next
        end

        it 'runs the command again while using the same read preference and caching the resume token' do
          expect(document[:fullDocument][:a]).to eq(2)
          expect(change_stream_document[:resumeAfter]).to eq(document[:_id])
        end

        context 'when provided a session' do

          let(:options) do
            { session: session}
          end

          let(:session) do
            client.start_session
          end

          before do
            enum.next
          end

          it 'does not close the session' do
            expect(session.ended?).to be(false)
          end
        end
      end

      context 'when the error is a SocketError' do

        let(:error) do
          Mongo::Error::SocketError
        end

        it_behaves_like 'a change stream that encounters an error from a getMore'
      end

      context 'when the error is a SocketTimeoutError' do

        let(:error) do
          Mongo::Error::SocketTimeoutError
        end

        it_behaves_like 'a change stream that encounters an error from a getMore'
      end

      context "when the error is 'not master'" do

        let(:error) do
          Mongo::Error::OperationFailure.new('not master',
            Mongo::Operation::GetMore::Result.new([]))
        end

        it_behaves_like 'a change stream that encounters an error from a getMore'
      end

      context "when the error is 'node is recovering'" do

        let(:error) do
          Mongo::Error::OperationFailure.new('node is recovering',
            Mongo::Operation::GetMore::Result.new([]))
        end

        it_behaves_like 'a change stream that encounters an error from a getMore'
      end
    end

    context 'when the error is another server error' do

      before do
        change_stream
        collection.insert_one(a: 1)
        enum.next
        collection.insert_one(a: 2)
        expect(cursor).to receive(:get_more).and_raise(Mongo::Error::MissingResumeToken)
        expect(cursor).to receive(:kill_cursors).and_call_original
        expect(Mongo::Operation::Aggregate).not_to receive(:new)
      end

      let(:enum) do
        change_stream.to_enum
      end

      it 'does not run the command again and instead raises the error' do
        expect {
          enum.next
        }.to raise_exception(Mongo::Error::MissingResumeToken)
      end

      context 'when provided a session' do

        let(:options) do
          { session: session}
        end

        let(:session) do
          client.start_session
        end

        before do
          expect do
            enum.next
          end.to raise_error(Mongo::Error::MissingResumeToken)
        end

        it 'does not close the session' do
          expect(session.ended?).to be(false)
        end
      end
    end
  end

  describe '#inspect' do

    it 'includes the Ruby object_id in the formatted string' do
      expect(change_stream.inspect).to include(change_stream.object_id.to_s)
    end

    context 'when resume_after is provided' do

      let(:options) do
        { resume_after: sample_resume_token }
      end

      it 'includes resume_after value in the formatted string' do
        expect(change_stream.inspect).to include(sample_resume_token.to_s)
      end
    end

    context 'when max_await_time_ms is provided' do

      let(:options) do
        { max_await_time_ms: 10 }
      end

      it 'includes the max_await_time value in the formatted string' do
        expect(change_stream.inspect).to include({ max_await_time_ms: 10 }.to_s)
      end
    end

    context 'when batch_size is provided' do

      let(:options) do
        { batch_size: 5 }
      end

      it 'includes the batch_size value in the formatted string' do
        expect(change_stream.inspect).to include({ batch_size: 5 }.to_s)
      end
    end

    context 'when collation is provided'  do

      let(:options) do
        { 'collation' => { locale: 'en_US', strength: 2 } }
      end

      it 'includes the collation value in the formatted string' do
        expect(change_stream.inspect).to include({ 'collation' => { locale: 'en_US', strength: 2 } }.to_s)
      end
    end

    context 'when pipeline operators are provided' do

      let(:pipeline) do
        [{ '$project' => { '_id' => 0 }}]
      end

      it 'includes the filters in the formatted string' do
        expect(change_stream.inspect).to include([{ '$project' => { '_id' => 0 }}].to_s)
      end
    end
  end
end
