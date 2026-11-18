# frozen_string_literal: true

require 'spec_helper'

require 'opentelemetry/sdk'

# RUBY-3993: the tracer used to keep the OpenTelemetry context of every new
# cursor in a per-client map that nothing ever read. A cursor abandoned before
# it was exhausted left its entry, and the span it referenced, in the map for
# the life of the client.
describe 'OpenTelemetry tracer cursor state' do
  let(:exporter) { OpenTelemetry::SDK::Trace::Export::InMemorySpanExporter.new }
  let(:tracer_provider) do
    OpenTelemetry::SDK::Trace::TracerProvider.new.tap do |provider|
      provider.add_span_processor(
        OpenTelemetry::SDK::Trace::Export::SimpleSpanProcessor.new(exporter)
      )
    end
  end
  let(:otel_tracer) { tracer_provider.tracer('tracing-state-spec') }

  # A new client rather than authorized_client.with: a clone keeps the
  # original's tracer, which has tracing disabled.
  let(:client) do
    new_local_client(
      SpecConfig.instance.addresses,
      SpecConfig.instance.all_test_options.merge(
        database: SpecConfig.instance.test_db,
        tracing: { enabled: true, tracer: otel_tracer }
      )
    )
  end
  let(:collection_name) { 'tracing_state_spec' }
  let(:collection) { client[collection_name] }
  let(:tracer) { client.tracer }

  before do
    authorized_client[collection_name].drop
    authorized_client[collection_name].insert_many(Array.new(10) { |i| { _id: i } })
  end

  after do
    authorized_client[collection_name].drop
  end

  # The cursor id the server assigned to the query. Taking it from the
  # exported find span keeps the assertions independent of tracer internals.
  def cursor_id
    find_span.attributes['db.mongodb.cursor_id']
  end

  def find_span
    exporter.finished_spans.find { |span| span.attributes['db.operation.name'] == 'find' }
  end

  def getmore_spans
    exporter.finished_spans.select { |span| span.attributes['db.command.name'] == 'getMore' }
  end

  # Check the tracer's direct hash instance variables for the cursor id,
  # regardless of the name of the map that retained it. This does not detect
  # retention in nested objects or other kinds of state.
  def retained_cursor_keys
    tracer.instance_variables.flat_map do |name|
      state = tracer.instance_variable_get(name)
      state.is_a?(Hash) ? state.keys : []
    end
  end

  # A batch size smaller than the document count guarantees that the server
  # returns a non-zero cursor id, so the assertions below are not vacuous.
  context 'when iteration stops before the cursor is exhausted' do
    it 'does not retain the cursor context' do
      view = collection.find({}, batch_size: 2)
      view.each.next

      expect(cursor_id).to be > 0
      expect(retained_cursor_keys).not_to include(cursor_id)
    end
  end

  context 'when only the first document is read' do
    it 'does not retain the cursor context' do
      collection.find({}, batch_size: 2).first

      expect(cursor_id).to be > 0
      expect(retained_cursor_keys).not_to include(cursor_id)
    end
  end

  context 'when the cursor is killed' do
    it 'does not retain the cursor context' do
      view = collection.find({}, batch_size: 2)
      view.each.next
      view.close_query

      expect(cursor_id).to be > 0
      expect(retained_cursor_keys).not_to include(cursor_id)
    end
  end

  context 'when the cursor is iterated to the end' do
    it 'does not retain the cursor context' do
      expect(collection.find({}, batch_size: 2).to_a.length).to eq(10)

      expect(cursor_id).to be > 0
      expect(retained_cursor_keys).not_to include(cursor_id)
    end

    # The cursor id on getMore spans is set by the command tracer, not by the
    # removed cursor map, so it has to keep working.
    it 'keeps the cursor id on getMore spans' do
      collection.find({}, batch_size: 2).to_a
      cursor_ids = getmore_spans.map { |span| span.attributes['db.mongodb.cursor_id'] }

      expect(getmore_spans).not_to be_empty
      expect(cursor_ids.uniq.length).to eq(1)
      expect(cursor_ids.first).to be > 0
    end
  end
end
