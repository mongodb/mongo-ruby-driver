# frozen_string_literal: true

require 'spec_helper'

require 'opentelemetry'

describe Mongo::Tracing::OpenTelemetry::CommandTracer do
  let(:otel_tracer) { double('OpenTelemetry::Trace::Tracer') }
  let(:parent_tracer) { double('Mongo::Tracing::OpenTelemetry::Tracer') }
  let(:query_text_max_length) { 0 }
  let(:command_tracer) do
    described_class.new(otel_tracer, parent_tracer, query_text_max_length: query_text_max_length)
  end
  let(:lsid_value) { "55dcab94-2c82-445a-a7f2-5ce50213b753" }

  let(:connection) do
    double('Mongo::Server::Connection',
           id: 123,
           address: double('Address', host: 'localhost', port: 27_017),
           transport: :tcp,
           server: double('Server',
                          description: double('Description', server_connection_id: 456)))
  end

  let(:message) do
    double('Mongo::Protocol::Message',
           documents: [ document ],
           payload: { 'command' => document })
  end

  let(:document) do
    {
      'find' => 'users',
      '$db' => 'test_db',
      'lsid' => { 'id' => BSON::Binary.from_uuid(lsid_value) },
      'filter' => { 'name' => 'Alice' }
    }
  end

  let(:operation_context) { double('Mongo::Operation::Context') }

  describe '#initialize' do
    it 'sets the otel_tracer' do
      expect(command_tracer.instance_variable_get(:@otel_tracer)).to eq(otel_tracer)
    end

    it 'sets the parent_tracer' do
      expect(command_tracer.instance_variable_get(:@parent_tracer)).to eq(parent_tracer)
    end

    it 'sets the query_text_max_length' do
      expect(command_tracer.instance_variable_get(:@query_text_max_length)).to eq(0)
    end

    context 'with custom query_text_max_length' do
      let(:query_text_max_length) { 100 }

      it 'sets the custom query_text_max_length' do
        expect(command_tracer.instance_variable_get(:@query_text_max_length)).to eq(100)
      end
    end
  end

  describe '#trace_command' do
    let(:span) { double('OpenTelemetry::Trace::Span', finish: nil, set_attribute: nil) }
    let(:context) { double('OpenTelemetry::Context') }
    let(:result) { double('Result', has_cursor_id?: false, successful?: true) }

    before do
      allow(otel_tracer).to receive(:start_span).and_return(span)
      allow(OpenTelemetry::Trace).to receive(:with_span).and_yield(span, context)
    end

    it 'starts a span with the command name' do
      expect(otel_tracer).to receive(:start_span).with(
        'find',
        hash_including(kind: :client)
      )
      command_tracer.trace_command(message, operation_context, connection) { result }
    end

    it 'yields the block' do
      yielded = false
      command_tracer.trace_command(message, operation_context, connection) do
        yielded = true
        result
      end
      expect(yielded).to be true
    end

    it 'returns the block result' do
      return_value = command_tracer.trace_command(message, operation_context, connection) { result }
      expect(return_value).to eq(result)
    end

    it 'finishes the span' do
      expect(span).to receive(:finish)
      command_tracer.trace_command(message, operation_context, connection) { result }
    end

    context 'when result has cursor_id' do
      let(:result) do
        double('Result', has_cursor_id?: true, cursor_id: 789, successful?: true)
      end

      it 'sets the cursor_id attribute' do
        expect(span).to receive(:set_attribute).with('db.mongodb.cursor_id', 789)
        command_tracer.trace_command(message, operation_context, connection) { result }
      end
    end

    context 'when result has zero cursor_id' do
      let(:result) do
        double('Result', has_cursor_id?: true, cursor_id: 0, successful?: true)
      end

      it 'does not set the cursor_id attribute' do
        expect(span).not_to receive(:set_attribute).with('db.mongodb.cursor_id', anything)
        command_tracer.trace_command(message, operation_context, connection) { result }
      end
    end

    context 'when result is not successful' do
      let(:result) do
        double('Result',
               has_cursor_id?: false,
               successful?: false,
               error: double('Error', code: 13))
      end

      it 'sets the error status code' do
        expect(span).to receive(:set_attribute).with('db.response.status_code', '13')
        command_tracer.trace_command(message, operation_context, connection) { result }
      end
    end

    context 'when an OperationFailure exception is raised' do
      let(:error) { Mongo::Error::OperationFailure.new('error', nil, code: 42) }

      before do
        allow(span).to receive(:record_exception)
        allow(span).to receive(:status=)
      end

      it 'sets the error status code attribute' do
        expect(span).to receive(:set_attribute).with('db.response.status_code', '42')
        expect do
          command_tracer.trace_command(message, operation_context, connection) { raise error }
        end.to raise_error(error)
      end

      it 'records the exception' do
        expect(span).to receive(:record_exception).with(error)
        expect do
          command_tracer.trace_command(message, operation_context, connection) { raise error }
        end.to raise_error(error)
      end

      it 'sets the error status' do
        expect(span).to receive(:status=)
        expect do
          command_tracer.trace_command(message, operation_context, connection) { raise error }
        end.to raise_error(error)
      end

      it 'finishes the span' do
        expect(span).to receive(:finish)
        expect do
          command_tracer.trace_command(message, operation_context, connection) { raise error }
        end.to raise_error(error)
      end
    end

    context 'when a generic exception is raised' do
      let(:error) { StandardError.new('generic error') }

      before do
        allow(span).to receive(:record_exception)
        allow(span).to receive(:status=)
      end

      it 'does not set status code attribute' do
        expect(span).not_to receive(:set_attribute).with('db.response.status_code', anything)
        expect do
          command_tracer.trace_command(message, operation_context, connection) { raise error }
        end.to raise_error(error)
      end

      it 'records the exception' do
        expect(span).to receive(:record_exception).with(error)
        expect do
          command_tracer.trace_command(message, operation_context, connection) { raise error }
        end.to raise_error(error)
      end

      it 'sets the error status' do
        expect(span).to receive(:status=)
        expect do
          command_tracer.trace_command(message, operation_context, connection) { raise error }
        end.to raise_error(error)
      end

      it 'finishes the span' do
        expect(span).to receive(:finish)
        expect do
          command_tracer.trace_command(message, operation_context, connection) { raise error }
        end.to raise_error(error)
      end
    end
  end

  describe '#span_attributes' do
    subject { command_tracer.send(:span_attributes, message, connection) }

    it 'includes db.system' do
      expect(subject['db.system']).to eq('mongodb')
    end

    it 'includes db.namespace' do
      expect(subject['db.namespace']).to eq('test_db')
    end

    it 'includes db.collection.name' do
      expect(subject['db.collection.name']).to eq('users')
    end

    it 'includes db.command.name' do
      expect(subject['db.command.name']).to eq('find')
    end

    it 'includes db.query.summary' do
      expect(subject['db.query.summary']).to eq('find test_db.users')
    end

    it 'includes server.port' do
      expect(subject['server.port']).to eq(27_017)
    end

    it 'includes server.address' do
      expect(subject['server.address']).to eq('localhost')
    end

    it 'includes network.transport' do
      expect(subject['network.transport']).to eq('tcp')
    end

    it 'includes db.mongodb.server_connection_id' do
      expect(subject['db.mongodb.server_connection_id']).to eq(456)
    end

    it 'includes db.mongodb.driver_connection_id' do
      expect(subject['db.mongodb.driver_connection_id']).to eq(123)
    end

    it 'includes db.mongodb.lsid' do
      expect(subject['db.mongodb.lsid']).to eq(lsid_value)
    end

    it 'does not include nil values' do
      expect(subject).not_to have_key('db.mongodb.cursor_id')
      expect(subject).not_to have_key('db.mongodb.txn_number')
      expect(subject).not_to have_key('db.query.text')
    end

    context 'with getMore command' do
      let(:document) do
        {
          'getMore' => double('BSON::Int64', value: 999),
          'collection' => 'users',
          '$db' => 'test_db'
        }
      end

      it 'includes db.mongodb.cursor_id' do
        expect(subject['db.mongodb.cursor_id']).to eq(999)
      end
    end

    context 'with transaction number' do
      let(:document) do
        {
          'find' => 'users',
          '$db' => 'test_db',
          'txnNumber' => double('BSON::Int64', value: 42)
        }
      end

      it 'includes db.mongodb.txn_number' do
        expect(subject['db.mongodb.txn_number']).to eq(42)
      end
    end

    context 'with query text enabled' do
      let(:query_text_max_length) { 1000 }

      it 'includes db.query.text' do
        expect(subject['db.query.text']).to be_a(String)
        expect(subject['db.query.text']).to include('find')
      end
    end
  end

  describe '#collection_name' do
    subject { command_tracer.send(:collection_name, message) }

    context 'with find command' do
      let(:document) { { 'find' => 'users' } }

      it 'returns the collection name' do
        expect(subject).to eq('users')
      end
    end

    context 'with getMore command' do
      let(:document) { { 'getMore' => 123, 'collection' => 'users' } }

      it 'returns the collection name' do
        expect(subject).to eq('users')
      end
    end

    context 'with listCollections command' do
      let(:document) { { 'listCollections' => 1 } }

      it 'returns nil' do
        expect(subject).to be_nil
      end
    end

    context 'with listDatabases command' do
      let(:document) { { 'listDatabases' => 1 } }

      it 'returns nil' do
        expect(subject).to be_nil
      end
    end

    context 'with commitTransaction command' do
      let(:document) { { 'commitTransaction' => 1 } }

      it 'returns nil' do
        expect(subject).to be_nil
      end
    end

    context 'with abortTransaction command' do
      let(:document) { { 'abortTransaction' => 1 } }

      it 'returns nil' do
        expect(subject).to be_nil
      end
    end

    context 'with admin command with numeric value' do
      let(:document) { { 'serverStatus' => 1 } }

      it 'returns nil' do
        expect(subject).to be_nil
      end
    end
  end

  describe '#command_name' do
    subject { command_tracer.send(:command_name, message) }

    let(:document) { { 'find' => 'users' } }

    it 'returns the command name' do
      expect(subject).to eq('find')
    end
  end

  describe '#database' do
    subject { command_tracer.send(:database, message) }

    let(:document) { { 'find' => 'users', '$db' => 'test_db' } }

    it 'returns the database name' do
      expect(subject).to eq('test_db')
    end
  end

  describe '#query_summary' do
    subject { command_tracer.send(:query_summary, message) }

    context 'with collection name' do
      let(:document) { { 'find' => 'users', '$db' => 'test_db' } }

      it 'includes collection name' do
        expect(subject).to eq('find test_db.users')
      end
    end

    context 'without collection name' do
      let(:document) { { 'listCollections' => 1, '$db' => 'test_db' } }

      it 'does not include collection name' do
        expect(subject).to eq('listCollections test_db')
      end
    end
  end

  describe '#cursor_id' do
    subject { command_tracer.send(:cursor_id, message) }

    context 'with getMore command' do
      let(:document) { { 'getMore' => double('BSON::Int64', value: 999) } }

      it 'returns the cursor ID' do
        expect(subject).to eq(999)
      end
    end

    context 'with find command' do
      let(:document) { { 'find' => 'users' } }

      it 'returns nil' do
        expect(subject).to be_nil
      end
    end
  end

  describe '#lsid' do
    subject { command_tracer.send(:lsid, message) }

    context 'with lsid present' do
      let(:document) { { 'find' => 'users', 'lsid' => { 'id' => BSON::Binary.from_uuid(lsid_value) } } }

      it 'returns the session ID' do
        expect(subject).to eq(lsid_value)
      end
    end

    context 'without lsid' do
      let(:document) { { 'find' => 'users' } }

      it 'returns nil' do
        expect(subject).to be_nil
      end
    end
  end

  describe '#txn_number' do
    subject { command_tracer.send(:txn_number, message) }

    context 'with txnNumber present' do
      let(:document) { { 'find' => 'users', 'txnNumber' => double('BSON::Int64', value: 42) } }

      it 'returns the transaction number' do
        expect(subject).to eq(42)
      end
    end

    context 'without txnNumber' do
      let(:document) { { 'find' => 'users' } }

      it 'returns nil' do
        expect(subject).to be_nil
      end
    end
  end

  describe '#query_text' do
    subject { command_tracer.send(:query_text, message) }

    context 'when query text is disabled' do
      let(:query_text_max_length) { 0 }

      it 'returns nil' do
        expect(subject).to be_nil
      end
    end

    context 'when query text is enabled' do
      let(:query_text_max_length) { 1000 }
      let(:document) do
        {
          'find' => 'users',
          '$db' => 'test_db',
          'lsid' => { 'id' => BSON::Binary.from_uuid(lsid_value) },
          'filter' => { 'name' => 'Alice' }
        }
      end

      it 'returns JSON string without excluded keys' do
        json = subject
        expect(json).to be_a(String)
        parsed = JSON.parse(json)
        expect(parsed).to have_key('find')
        expect(parsed).to have_key('filter')
        expect(parsed).not_to have_key('lsid')
        expect(parsed).not_to have_key('$db')
      end

      context 'when query text exceeds max length' do
        let(:query_text_max_length) { 10 }

        it 'truncates with ellipsis' do
          expect(subject).to end_with('...')
          expect(subject.length).to eq(13) # 10 chars + '...'
        end
      end
    end
  end

  describe '#query_text?' do
    subject { command_tracer.send(:query_text?) }

    context 'when query_text_max_length is 0' do
      let(:query_text_max_length) { 0 }

      it 'returns false' do
        expect(subject).to be false
      end
    end

    context 'when query_text_max_length is positive' do
      let(:query_text_max_length) { 100 }

      it 'returns true' do
        expect(subject).to be true
      end
    end
  end
end
