# frozen_string_literal: true

require 'spec_helper'

require 'opentelemetry'

describe Mongo::Tracing::OpenTelemetry::OperationTracer do
  # rubocop:disable RSpec/VerifiedDoubles
  let(:otel_tracer) { double('OpenTelemetry::Trace::Tracer') }
  let(:parent_tracer) do
    instance_double(
      Mongo::Tracing::OpenTelemetry::Tracer,
      cursor_context_map: cursor_context_map,
      parent_context_for: parent_context,
      transaction_context_map: transaction_context_map,
      transaction_map_key: transaction_key
    )
  end
  let(:cursor_context_map) { {} }
  let(:transaction_context_map) { {} }
  let(:parent_context) { nil }
  let(:transaction_key) { nil }
  let(:operation_tracer) { described_class.new(otel_tracer, parent_tracer) }

  let(:operation_context) { instance_double(Mongo::Operation::Context, session: session) }
  let(:session) { instance_double(Mongo::Session) }

  describe '#initialize' do
    it 'sets the otel_tracer' do
      expect(operation_tracer.instance_variable_get(:@otel_tracer)).to eq(otel_tracer)
    end

    it 'sets the parent_tracer' do
      expect(operation_tracer.instance_variable_get(:@parent_tracer)).to eq(parent_tracer)
    end
  end

  describe '#trace_operation' do
    let(:span) do
      double('OpenTelemetry::Trace::Span').tap do |s|
        allow(s).to receive(:finish)
        allow(s).to receive(:set_attribute)
        allow(s).to receive(:record_exception)
        allow(s).to receive(:status=)
      end
    end
    let(:context) { double('OpenTelemetry::Context') }
    let(:result) { double('Result', is_a?: false) }
    # rubocop:enable RSpec/VerifiedDoubles
    let(:operation_class) { class_double(Mongo::Operation::Find, name: 'Mongo::Operation::Find') }
    let(:operation) do
      instance_double(
        Mongo::Operation::Find,
        db_name: 'test_db',
        coll_name: 'test_collection',
        cursor_id: nil,
        class: operation_class,
        respond_to?: true
      )
    end

    before do
      allow(otel_tracer).to receive(:start_span).and_return(span)
      allow(OpenTelemetry::Trace).to receive(:with_span).and_yield(span, context)
      allow(parent_tracer).to receive(:parent_context_for).and_return(nil)
    end

    it 'starts a span with the operation name' do
      expect(otel_tracer).to receive(:start_span).with(
        'find test_db.test_collection',
        hash_including(kind: :client, with_parent: nil)
      )
      operation_tracer.trace_operation(operation, operation_context) { result }
    end

    it 'yields the block' do
      yielded = false
      operation_tracer.trace_operation(operation, operation_context) do
        yielded = true
        result
      end
      expect(yielded).to be true
    end

    it 'returns the block result' do
      return_value = operation_tracer.trace_operation(operation, operation_context) { result }
      expect(return_value).to eq(result)
    end

    it 'finishes the span' do
      expect(span).to receive(:finish)
      operation_tracer.trace_operation(operation, operation_context) { result }
    end

    context 'with custom operation name' do
      it 'uses the provided op_name' do
        expect(otel_tracer).to receive(:start_span).with(
          'custom_op test_db.test_collection',
          hash_including(kind: :client)
        )
        operation_tracer.trace_operation(operation, operation_context, op_name: 'custom_op') { result }
      end
    end

    context 'with parent context' do
      # rubocop:disable RSpec/VerifiedDoubles
      let(:parent_context) { double('OpenTelemetry::Context') }
      # rubocop:enable RSpec/VerifiedDoubles

      before do
        allow(parent_tracer).to receive(:parent_context_for).and_return(parent_context)
      end

      it 'uses the parent context' do
        expect(otel_tracer).to receive(:start_span).with(
          anything,
          hash_including(with_parent: parent_context)
        )
        operation_tracer.trace_operation(operation, operation_context) { result }
      end
    end

    context 'when result is a Cursor with new cursor_id' do
      let(:cursor) do
        instance_double(
          Mongo::Cursor,
          is_a?: true,
          id: 12_345
        )
      end

      before do
        allow(cursor).to receive(:is_a?).with(Mongo::Cursor).and_return(true)
      end

      it 'stores cursor context in map' do
        operation_tracer.trace_operation(operation, operation_context) { cursor }
        expect(cursor_context_map[12_345]).to eq(context)
      end

      it 'sets the cursor_id attribute on span' do
        expect(span).to receive(:set_attribute).with('db.mongodb.cursor_id', 12_345)
        operation_tracer.trace_operation(operation, operation_context) { cursor }
      end
    end

    context 'when result is a Cursor with closed cursor (id = 0)' do
      let(:cursor_id) { 999 }
      let(:cursor) do
        instance_double(
          Mongo::Cursor,
          is_a?: true,
          id: 0
        )
      end
      let(:operation_class) { class_double(Mongo::Operation::GetMore, name: 'Mongo::Operation::GetMore') }
      let(:operation) do
        instance_double(
          Mongo::Operation::GetMore,
          db_name: 'test_db',
          coll_name: 'test_collection',
          cursor_id: cursor_id,
          class: operation_class,
          respond_to?: true
        )
      end

      before do
        allow(cursor).to receive(:is_a?).with(Mongo::Cursor).and_return(true)
        # rubocop:disable RSpec/VerifiedDoubles
        cursor_context_map[cursor_id] = double('OpenTelemetry::Context')
        # rubocop:enable RSpec/VerifiedDoubles
      end

      it 'removes cursor from context map' do
        operation_tracer.trace_operation(operation, operation_context) { cursor }
        expect(cursor_context_map).not_to have_key(cursor_id)
      end

      it 'does not set cursor_id attribute' do
        expect(span).not_to receive(:set_attribute).with('db.mongodb.cursor_id', anything)
        operation_tracer.trace_operation(operation, operation_context) { cursor }
      end
    end

    context 'when an exception is raised' do
      let(:error) { StandardError.new('test error') }

      it 'records the exception' do
        expect(span).to receive(:record_exception).with(error)
        expect do
          operation_tracer.trace_operation(operation, operation_context) { raise error }
        end.to raise_error(error)
      end

      it 'sets the error status' do
        expect(span).to receive(:status=) do |status|
          expect(status).to be_a(OpenTelemetry::Trace::Status)
          expect(status.description).to match(/Unhandled exception of type: StandardError/)
        end
        expect do
          operation_tracer.trace_operation(operation, operation_context) { raise error }
        end.to raise_error(error)
      end

      it 'finishes the span' do
        expect(span).to receive(:finish)
        expect do
          operation_tracer.trace_operation(operation, operation_context) { raise error }
        end.to raise_error(error)
      end

      it 'reraises the exception' do
        expect do
          operation_tracer.trace_operation(operation, operation_context) { raise error }
        end.to raise_error(error)
      end
    end
  end

  describe '#span_attributes' do
    subject(:attributes) { operation_tracer.send(:span_attributes, operation, op_name) }

    let(:op_name) { nil }
    let(:operation_class) { class_double(Mongo::Operation::Find, name: 'Mongo::Operation::Find') }
    let(:operation) do
      instance_double(
        Mongo::Operation::Find,
        db_name: 'test_db',
        coll_name: 'users',
        cursor_id: nil,
        class: operation_class,
        respond_to?: true
      )
    end

    it 'includes db.system' do
      expect(attributes['db.system']).to eq('mongodb')
    end

    it 'includes db.namespace' do
      expect(attributes['db.namespace']).to eq('test_db')
    end

    it 'includes db.collection.name' do
      expect(attributes['db.collection.name']).to eq('users')
    end

    it 'includes db.operation.name' do
      expect(attributes['db.operation.name']).to eq('find')
    end

    it 'includes db.operation.summary' do
      expect(attributes['db.operation.summary']).to eq('find test_db.users')
    end

    it 'does not include nil values' do
      expect(attributes).not_to have_key('db.mongodb.cursor_id')
    end

    context 'with cursor_id' do
      let(:operation_class) { class_double(Mongo::Operation::GetMore, name: 'Mongo::Operation::GetMore') }
      let(:operation) do
        instance_double(
          Mongo::Operation::GetMore,
          db_name: 'test_db',
          coll_name: 'users',
          cursor_id: 12_345,
          class: operation_class,
          respond_to?: true
        )
      end

      it 'includes db.mongodb.cursor_id' do
        expect(attributes['db.mongodb.cursor_id']).to eq(12_345)
      end
    end

    context 'with custom op_name' do
      let(:op_name) { 'custom_operation' }

      it 'uses the custom op_name' do
        expect(attributes['db.operation.name']).to eq('custom_operation')
      end
    end
  end

  describe '#operation_name' do
    subject(:op_name_result) { operation_tracer.send(:operation_name, operation, op_name) }

    let(:op_name) { nil }
    let(:operation_class) { class_double(Mongo::Operation::Find, name: 'Mongo::Operation::Find') }
    let(:operation) do
      instance_double(Mongo::Operation::Find, class: operation_class)
    end

    it 'returns the operation class name in lowercase' do
      expect(op_name_result).to eq('find')
    end

    context 'with custom op_name' do
      let(:op_name) { 'CustomOperation' }

      it 'returns the custom op_name' do
        expect(op_name_result).to eq('CustomOperation')
      end
    end
  end

  describe '#collection_name' do
    subject(:coll_name) { operation_tracer.send(:collection_name, operation) }

    context 'when operation responds to coll_name and has a value' do
      let(:operation) do
        instance_double(Mongo::Operation::Find, coll_name: 'test_collection', respond_to?: true)
      end

      it 'returns the coll_name' do
        expect(coll_name).to eq('test_collection')
      end
    end

    context 'when operation does not respond to coll_name' do
      context 'with Aggregate operation' do
        let(:operation) do
          instance_double(
            Mongo::Operation::Aggregate,
            respond_to?: false,
            spec: { selector: { aggregate: 'agg_collection' } }
          )
        end

        before do
          allow(Mongo::Operation::Aggregate).to receive(:===).with(operation).and_return(true)
          allow(Mongo::Operation::Count).to receive(:===).with(operation).and_return(false)
          allow(Mongo::Operation::Create).to receive(:===).with(operation).and_return(false)
          allow(Mongo::Operation::Distinct).to receive(:===).with(operation).and_return(false)
          allow(Mongo::Operation::Drop).to receive(:===).with(operation).and_return(false)
          allow(Mongo::Operation::WriteCommand).to receive(:===).with(operation).and_return(false)
        end

        it 'returns the aggregate collection name' do
          expect(coll_name).to eq('agg_collection')
        end
      end

      context 'with Count operation' do
        let(:operation) do
          instance_double(
            Mongo::Operation::Count,
            respond_to?: false,
            spec: { selector: { count: 'count_collection' } }
          )
        end

        before do
          allow(Mongo::Operation::Aggregate).to receive(:===).with(operation).and_return(false)
          allow(Mongo::Operation::Count).to receive(:===).with(operation).and_return(true)
          allow(Mongo::Operation::Create).to receive(:===).with(operation).and_return(false)
          allow(Mongo::Operation::Distinct).to receive(:===).with(operation).and_return(false)
          allow(Mongo::Operation::Drop).to receive(:===).with(operation).and_return(false)
          allow(Mongo::Operation::WriteCommand).to receive(:===).with(operation).and_return(false)
        end

        it 'returns the count collection name' do
          expect(coll_name).to eq('count_collection')
        end
      end

      context 'with Create operation' do
        let(:operation) do
          instance_double(
            Mongo::Operation::Create,
            respond_to?: false,
            spec: { selector: { create: 'new_collection' } }
          )
        end

        before do
          allow(Mongo::Operation::Aggregate).to receive(:===).with(operation).and_return(false)
          allow(Mongo::Operation::Count).to receive(:===).with(operation).and_return(false)
          allow(Mongo::Operation::Create).to receive(:===).with(operation).and_return(true)
          allow(Mongo::Operation::Distinct).to receive(:===).with(operation).and_return(false)
          allow(Mongo::Operation::Drop).to receive(:===).with(operation).and_return(false)
          allow(Mongo::Operation::WriteCommand).to receive(:===).with(operation).and_return(false)
        end

        it 'returns the create collection name' do
          expect(coll_name).to eq('new_collection')
        end
      end

      context 'with Distinct operation' do
        let(:operation) do
          instance_double(
            Mongo::Operation::Distinct,
            respond_to?: false,
            spec: { selector: { distinct: 'distinct_collection' } }
          )
        end

        before do
          allow(Mongo::Operation::Aggregate).to receive(:===).with(operation).and_return(false)
          allow(Mongo::Operation::Count).to receive(:===).with(operation).and_return(false)
          allow(Mongo::Operation::Create).to receive(:===).with(operation).and_return(false)
          allow(Mongo::Operation::Distinct).to receive(:===).with(operation).and_return(true)
          allow(Mongo::Operation::Drop).to receive(:===).with(operation).and_return(false)
          allow(Mongo::Operation::WriteCommand).to receive(:===).with(operation).and_return(false)
        end

        it 'returns the distinct collection name' do
          expect(coll_name).to eq('distinct_collection')
        end
      end

      context 'with Drop operation' do
        let(:operation) do
          instance_double(
            Mongo::Operation::Drop,
            respond_to?: false,
            spec: { selector: { drop: 'dropped_collection' } }
          )
        end

        before do
          allow(Mongo::Operation::Aggregate).to receive(:===).with(operation).and_return(false)
          allow(Mongo::Operation::Count).to receive(:===).with(operation).and_return(false)
          allow(Mongo::Operation::Create).to receive(:===).with(operation).and_return(false)
          allow(Mongo::Operation::Distinct).to receive(:===).with(operation).and_return(false)
          allow(Mongo::Operation::Drop).to receive(:===).with(operation).and_return(true)
          allow(Mongo::Operation::WriteCommand).to receive(:===).with(operation).and_return(false)
        end

        it 'returns the drop collection name' do
          expect(coll_name).to eq('dropped_collection')
        end
      end

      context 'with WriteCommand operation' do
        let(:operation) do
          instance_double(
            Mongo::Operation::WriteCommand,
            respond_to?: false,
            spec: { selector: { insert: 'write_collection' } }
          )
        end

        before do
          allow(Mongo::Operation::Aggregate).to receive(:===).with(operation).and_return(false)
          allow(Mongo::Operation::Count).to receive(:===).with(operation).and_return(false)
          allow(Mongo::Operation::Create).to receive(:===).with(operation).and_return(false)
          allow(Mongo::Operation::Distinct).to receive(:===).with(operation).and_return(false)
          allow(Mongo::Operation::Drop).to receive(:===).with(operation).and_return(false)
          allow(Mongo::Operation::WriteCommand).to receive(:===).with(operation).and_return(true)
        end

        it 'returns the first value from selector' do
          expect(coll_name).to eq('write_collection')
        end
      end

      context 'with unknown operation type' do
        let(:operation) do
          instance_double(Mongo::Operation::Find, respond_to?: false)
        end

        before do
          allow(operation).to receive(:is_a?).and_return(false)
        end

        it 'returns nil' do
          expect(coll_name).to be_nil
        end
      end
    end

    context 'when coll_name is nil' do
      let(:operation) do
        instance_double(
          Mongo::Operation::Find,
          coll_name: nil,
          respond_to?: true,
          spec: { selector: { listCollections: 1 } }
        )
      end

      before do
        stub_const('Mongo::Operation::Aggregate', Class.new)
        allow(operation).to receive(:is_a?).with(Mongo::Operation::Aggregate).and_return(false)
        allow(operation).to receive(:is_a?).and_return(false)
      end

      it 'returns nil' do
        expect(coll_name).to be_nil
      end
    end
  end

  describe '#operation_span_name' do
    subject(:span_name) { operation_tracer.send(:operation_span_name, operation, op_name) }

    let(:op_name) { nil }

    context 'with collection name' do
      let(:operation_class) { class_double(Mongo::Operation::Find, name: 'Mongo::Operation::Find') }
      let(:operation) do
        instance_double(
          Mongo::Operation::Find,
          db_name: 'test_db',
          coll_name: 'users',
          class: operation_class,
          respond_to?: true
        )
      end

      it 'includes collection name in format' do
        expect(span_name).to eq('find test_db.users')
      end
    end

    context 'without collection name' do
      let(:operation_class) do
        class_double(Mongo::Operation::ListCollections, name: 'Mongo::Operation::ListCollections')
      end
      let(:operation) do
        instance_double(
          Mongo::Operation::ListCollections,
          db_name: 'test_db',
          coll_name: nil,
          class: operation_class,
          respond_to?: true
        )
      end

      before do
        allow(operation).to receive(:is_a?).and_return(false)
      end

      it 'excludes collection name from format' do
        expect(span_name).to eq('listcollections test_db')
      end
    end

    context 'with empty collection name' do
      let(:operation_class) { class_double(Mongo::Operation::Command, name: 'Mongo::Operation::Command') }
      let(:operation) do
        instance_double(
          Mongo::Operation::Command,
          db_name: 'test_db',
          coll_name: '',
          class: operation_class,
          respond_to?: true
        )
      end

      it 'excludes collection name from format' do
        expect(span_name).to eq('command test_db')
      end
    end
  end

  describe '#process_cursor_context' do
    subject(:process_result) do
      operation_tracer.send(:process_cursor_context, result, cursor_id, context, span)
    end

    # rubocop:disable RSpec/VerifiedDoubles
    let(:context) { double('OpenTelemetry::Context') }
    let(:span) do
      double('OpenTelemetry::Trace::Span').tap do |s|
        allow(s).to receive(:set_attribute)
      end
    end
    let(:cursor_id) { nil }

    context 'when result is not a Cursor' do
      let(:result) { double('Result', is_a?: false) }
      # rubocop:enable RSpec/VerifiedDoubles

      it 'does not modify cursor_context_map' do
        expect { process_result }.not_to(change { cursor_context_map })
      end
    end

    context 'when result is a Cursor with zero id' do
      let(:cursor_id) { 123 }
      let(:result) do
        instance_double(Mongo::Cursor, is_a?: true, id: 0)
      end

      before do
        allow(result).to receive(:is_a?).with(Mongo::Cursor).and_return(true)
        # rubocop:disable RSpec/VerifiedDoubles
        cursor_context_map[cursor_id] = double('OpenTelemetry::Context')
        # rubocop:enable RSpec/VerifiedDoubles
      end

      it 'removes the cursor from context map' do
        process_result
        expect(cursor_context_map).not_to have_key(cursor_id)
      end

      it 'does not set cursor_id attribute on span' do
        expect(span).not_to receive(:set_attribute)
        process_result
      end
    end

    context 'when result is a Cursor with new id' do
      let(:result) do
        instance_double(Mongo::Cursor, is_a?: true, id: 456)
      end

      before do
        allow(result).to receive(:is_a?).with(Mongo::Cursor).and_return(true)
      end

      it 'stores the context in cursor_context_map' do
        process_result
        expect(cursor_context_map[456]).to eq(context)
      end

      it 'sets the cursor_id attribute on span' do
        expect(span).to receive(:set_attribute).with('db.mongodb.cursor_id', 456)
        process_result
      end
    end

    context 'when result is a Cursor with existing id' do
      let(:cursor_id) { 789 }
      let(:result) do
        instance_double(Mongo::Cursor, is_a?: true, id: 789)
      end

      before do
        allow(result).to receive(:is_a?).with(Mongo::Cursor).and_return(true)
        # rubocop:disable RSpec/VerifiedDoubles
        cursor_context_map[cursor_id] = double('OpenTelemetry::Context')
        # rubocop:enable RSpec/VerifiedDoubles
      end

      it 'does not update cursor_context_map' do
        expect { process_result }.not_to(change(cursor_context_map, :keys))
      end

      it 'does not set cursor_id attribute on span' do
        expect(span).not_to receive(:set_attribute)
        process_result
      end
    end
  end
end
