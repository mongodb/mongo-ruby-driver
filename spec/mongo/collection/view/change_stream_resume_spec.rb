# frozen_string_literal: true
# rubocop:todo all

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

  let(:cursor) do
    change_stream.instance_variable_get(:@cursor)
  end

  let(:change_stream_document) do
    change_stream.send(:instance_variable_set, '@resuming', false)
    change_stream.send(:pipeline)[0]['$changeStream']
  end

  let(:connection_description) do
    Mongo::Server::Description.new(
      double('description address'),
      { 'minWireVersion' => 0, 'maxWireVersion' => 2 }
    )
  end

  let(:result) do
    Mongo::Operation::GetMore::Result.new(
      Mongo::Protocol::Message.new,
      connection_description,
    )
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
    context 'using Enumerable' do
      require_mri

      before do
        change_stream
        collection.insert_one(a:1)
        enum.next
        collection.insert_one(a:2)
      end

      let(:enum) do
        change_stream.to_enum
      end

      it 'resumes on a cursor not found error' do
        original_cursor_id = cursor.id

        client.use(:admin).command({
          killCursors: collection.name,
          cursors: [cursor.id]
        })

        expect do
          enum.next
        end.not_to raise_error
      end
    end

    context 'using try_next' do
      before do
        change_stream
        collection.insert_one(a:1)
        expect(change_stream.try_next).to be_a(BSON::Document)
        collection.insert_one(a:2)
      end

      it 'resumes on a cursor not found error' do
        original_cursor_id = cursor.id

        client.use(:admin).command({
          killCursors: collection.name,
          cursors: [cursor.id]
        })

        expect do
          change_stream.try_next
        end.not_to raise_error
      end
    end
  end

  context 'when a server error is encountered during a getMore' do
    fails_on_jruby

    shared_examples_for 'a change stream that is not resumed' do
      before do
        change_stream
        collection.insert_one(a: 1)
        enum.next
        collection.insert_one(a: 2)
        expect(cursor).to receive(:get_more).once.and_raise(error)
      end

      let(:enum) do
        change_stream.to_enum
      end

      let(:document) do
        enum.next
      end

      it 'is not resumed' do
        expect do
          document
        end.to raise_error(error)
      end
    end

    context 'when the error is a resumable error' do

      shared_examples_for 'a change stream that encounters an error from a getMore' do

        before do
          change_stream
          collection.insert_one(a: 1)
          enum.next
          collection.insert_one(a: 2)
          expect(cursor).to receive(:get_more).once.and_raise(error)
        end

        let(:enum) do
          change_stream.to_enum
        end

        let(:document) do
          enum.next
        end

        it 'runs the command again while using the same read preference and caching the resume token' do
          expect(cursor).to receive(:close).and_call_original
          expect(view.send(:server_selector)).to receive(:select_server).once.and_call_original
          expect(Mongo::Operation::Aggregate).to receive(:new).and_call_original

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
          Mongo::Error::OperationFailure.new('not master', result)
        end

        it_behaves_like 'a change stream that is not resumed'
      end

      context "when the error is 'node is recovering'" do

        let(:error) do
          Mongo::Error::OperationFailure.new('node is recovering', result)
        end

        it_behaves_like 'a change stream that is not resumed'
      end
    end

    context 'when the error is another server error' do

      before do
        change_stream
        collection.insert_one(a: 1)
        enum.next
        collection.insert_one(a: 2)
        expect(cursor).to receive(:get_more).and_raise(Mongo::Error::MissingResumeToken)
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
end
