require 'spec_helper'

describe Mongo::Collection::View::ChangeStream, if: test_change_streams? do

  let(:pipeline) do
    []
  end

  let(:options) do
    {}
  end

  let(:view_options) do
    {}
  end

  let(:view) do
    Mongo::Collection::View.new(authorized_collection, {}, view_options)
  end

  let(:change_stream) do
    described_class.new(view, pipeline, options)
  end

  let(:change_stream_document) do
    change_stream.send(:pipeline)[0]['$changeStream']
  end

  let!(:sample_resume_token) do
    stream = authorized_collection.watch
    authorized_collection.insert_one(a: 1)
    doc = stream.to_enum.next
    stream.close
    doc[:_id]
  end

  let(:command_selector) do
    command_spec[:selector]
  end

  let(:command_spec) do
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
    end
  end

  after do
    authorized_collection.delete_many
    begin; change_stream.close; rescue; end
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
        authorized_collection.with(read_concern: { level: 'majority' })
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

        context 'when the operation fails', if: test_change_streams? do

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

          let(:client) do
            authorized_client
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
      end
    end

    context 'when provided a session', if: sessions_enabled? && test_change_streams? do

      let(:options) do
        { session: session }
      end

      let(:operation) do
        change_stream
        authorized_collection.insert_one(a: 1)
        change_stream.to_enum.next
      end

      let(:client) do
        authorized_client
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
          authorized_client_with_retry_writes.start_session
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

      before do
        change_stream
        authorized_collection.insert_one(a: 1)
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
      [{ '$project' => { _id: 0 } }]
    end

    before do
      change_stream
      authorized_collection.insert_one(a: 1)
    end

    it 'raises an exception and closes the cursor' do
      expect(cursor).to receive(:kill_cursors).and_call_original
      expect {
        change_stream.to_enum.next
      }.to raise_exception(Mongo::Error::MissingResumeToken)
    end
  end

  context 'when an error is encountered the first time the command is run' do

    let(:primary_socket) do
      primary = authorized_collection.client.cluster.servers.find { |s| s.primary? }
      connection = primary.pool.checkout
      primary.pool.checkin(connection)
      connection.send(:socket)
    end

    context 'when the error is a resumable error' do

      shared_examples_for 'a resumable change stream' do

        before do
          expect(primary_socket).to receive(:write).and_raise(error).once
          expect(view.send(:server_selector)).to receive(:select_server).twice.and_call_original
          change_stream
          authorized_collection.insert_one(a: 1)
        end

        let(:document) do
          change_stream.to_enum.next
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
            authorized_client.start_session
          end

          before do
            change_stream.to_enum.next
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

        it_behaves_like 'a resumable change stream'
      end

      context 'when the error is a SocketTimeoutError' do

        let(:error) do
          Mongo::Error::SocketTimeoutError
        end

        it_behaves_like 'a resumable change stream'
      end

      context "when the error is a 'not master' error" do

        let(:error) do
          Mongo::Error::OperationFailure.new('not master')
        end

        it_behaves_like 'a resumable change stream'
      end

      context "when the error is a 'cursor not found (43)' error" do

        let(:error) do
          Mongo::Error::OperationFailure.new('cursor not found (43)')
        end

        it_behaves_like 'a resumable change stream'
      end
    end

    context 'when the error is another server error' do

      before do
        expect(primary_socket).to receive(:write).and_raise(Mongo::Error::OperationFailure)
        #expect twice because of kill_cursors in after block
        expect(view.send(:server_selector)).to receive(:select_server).twice.and_call_original
      end

      it 'does not run the command again and instead raises the error' do
        expect {
          change_stream
        }.to raise_exception(Mongo::Error::OperationFailure)
      end

      context 'when provided a session' do

        let(:options) do
          { session: session}
        end

        let(:session) do
          authorized_client.start_session
        end

        before do
          begin; change_stream; rescue; end
        end

        it 'does not close the session' do
          expect(session.ended?).to be(false)
        end
      end
    end
  end

  context 'when a server error is encountered during a getMore' do

    context 'when the error is a resumable error' do

      shared_examples_for 'a change stream that encounters an error from a getMore' do

        before do
          change_stream
          authorized_collection.insert_one(a: 1)
          enum.next
          authorized_collection.insert_one(a: 2)
          expect(cursor).to receive(:get_more).once.and_raise(error)
          expect(cursor).to receive(:kill_cursors).and_call_original
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
            authorized_client.start_session
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

      context "when the error is a not 'master error'" do

        let(:error) do
          Mongo::Error::OperationFailure.new('not master')
        end

        it_behaves_like 'a change stream that encounters an error from a getMore'
      end

      context "when the error is a not 'cursor not found error'" do

        let(:error) do
          Mongo::Error::OperationFailure.new('cursor not found (43)')
        end

        it_behaves_like 'a change stream that encounters an error from a getMore'
      end
    end

    context 'when the error is another server error' do

      before do
        change_stream
        authorized_collection.insert_one(a: 1)
        enum.next
        authorized_collection.insert_one(a: 2)
        expect(cursor).to receive(:get_more).and_raise(Mongo::Error::OperationFailure)
        expect(cursor).to receive(:kill_cursors).and_call_original
        expect(Mongo::Operation::Aggregate).not_to receive(:new)
      end

      let(:enum) do
        change_stream.to_enum
      end

      it 'does not run the command again and instead raises the error' do
        expect {
          enum.next
        }.to raise_exception(Mongo::Error::OperationFailure)
      end

      context 'when provided a session' do

        let(:options) do
          { session: session}
        end

        let(:session) do
          authorized_client.start_session
        end

        before do
          begin; enum.next; rescue; end
        end

        it 'does not close the session' do
          expect(session.ended?).to be(false)
        end
      end
    end
  end

  context 'when a server error is encountered during the command following an error during getMore' do

    context 'when the error is a resumable error' do

      shared_examples_for 'a change stream that sent getMores, that then encounters an error when resuming' do

        before do
          change_stream
          authorized_collection.insert_one(a: 1)
          enum.next
          authorized_collection.insert_one(a: 2)
          expect(cursor).to receive(:get_more).and_raise(error)
          expect(cursor).to receive(:kill_cursors).and_call_original
          expect(change_stream).to receive(:send_initial_query).and_raise(error).once.ordered
        end

        let(:enum) do
          change_stream.to_enum
        end

        let(:document) do
          enum.next
        end

        it 'raises the error' do
          expect {
            document
          }.to raise_exception(error)
        end

        context 'when provided a session' do

          let(:options) do
            { session: session}
          end

          let(:session) do
            authorized_client.start_session
          end

          before do
            begin; document; rescue; end
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

        it_behaves_like 'a change stream that sent getMores, that then encounters an error when resuming'
      end

      context 'when the error is a SocketTimeoutError' do

        let(:error) do
          Mongo::Error::SocketTimeoutError
        end

        it_behaves_like 'a change stream that sent getMores, that then encounters an error when resuming'
      end

      context "when the error is a 'not master error'" do

        let(:error) do
          Mongo::Error::OperationFailure.new('not master')
        end

        it_behaves_like 'a change stream that sent getMores, that then encounters an error when resuming'
      end

      context "when the error is a not 'cursor not found error'" do

        let(:error) do
          Mongo::Error::OperationFailure.new('cursor not found (43)')
        end

        it_behaves_like 'a change stream that sent getMores, that then encounters an error when resuming'
      end
    end

    context 'when the error is another server error' do

      before do
        change_stream
        authorized_collection.insert_one(a: 1)
        enum.next
        authorized_collection.insert_one(a: 2)
        expect(cursor).to receive(:get_more).and_raise(Mongo::Error::OperationFailure.new('not master'))
        expect(cursor).to receive(:kill_cursors).and_call_original
        expect(change_stream).to receive(:send_initial_query).and_raise(Mongo::Error::OperationFailure).once.ordered
      end

      let(:enum) do
        change_stream.to_enum
      end

      it 'does not run the command again and instead raises the error' do
        expect {
          enum.next
        }.to raise_exception(Mongo::Error::OperationFailure)
      end

      context 'when provided a session' do

        let(:options) do
          { session: session}
        end

        let(:session) do
          authorized_client.start_session
        end

        before do
          begin; enum.next; rescue; end
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
