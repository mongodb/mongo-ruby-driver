# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Collection::View::Aggregation do

  let(:pipeline) do
    []
  end

  let(:view_options) do
    {}
  end

  let(:options) do
    {}
  end

  let(:selector) do
    {}
  end

  let(:view) do
    Mongo::Collection::View.new(authorized_collection, selector, view_options)
  end

  let(:aggregation) do
    described_class.new(view, pipeline, options)
  end

  let(:server) do
    double('server')
  end

  let(:session) do
    double('session')
  end

  let(:aggregation_spec) do
    aggregation.send(:aggregate_spec, session, nil)
  end

  before do
    authorized_collection.delete_many
  end

  describe '#allow_disk_use' do

    let(:new_agg) do
      aggregation.allow_disk_use(true)
    end

    it 'sets the value in the options' do
      expect(new_agg.allow_disk_use).to be true
    end
  end

  describe '#each' do

    let(:documents) do
      [
        { city: "Berlin", pop: 18913, neighborhood: "Kreuzberg" },
        { city: "Berlin", pop: 84143, neighborhood: "Mitte" },
        { city: "New York", pop: 40270, neighborhood: "Brooklyn" }
      ]
    end

    let(:pipeline) do
      [{
        "$group" => {
          "_id" => "$city",
          "totalpop" => { "$sum" => "$pop" }
        }
      }]
    end

    before do
      authorized_collection.delete_many
      authorized_collection.insert_many(documents)
    end

    context 'when provided a session' do

      let(:options) do
        { session: session }
      end

      let(:operation) do
        aggregation.to_a
      end

      let(:client) do
        authorized_client
      end

      it_behaves_like 'an operation using a session'
    end

    context 'when a block is provided' do

      context 'when no batch size is provided' do

        it 'yields to each document' do
          aggregation.each do |doc|
            expect(doc[:totalpop]).to_not be_nil
          end
        end
      end

      context 'when a batch size of 0 is provided' do

        let(:aggregation) do
          described_class.new(view.batch_size(0), pipeline, options)
        end

        it 'yields to each document' do
          aggregation.each do |doc|
            expect(doc[:totalpop]).to_not be_nil
          end
        end
      end

      context 'when a batch size of greater than zero is provided' do

        let(:aggregation) do
          described_class.new(view.batch_size(5), pipeline, options)
        end

        it 'yields to each document' do
          aggregation.each do |doc|
            expect(doc[:totalpop]).to_not be_nil
          end
        end
      end
    end

    context 'when no block is provided' do

      it 'returns an enumerated cursor' do
        expect(aggregation.each).to be_a(Enumerator)
      end
    end

    context 'when an invalid pipeline operator is provided' do

      let(:pipeline) do
        [{ '$invalid' => 'operator' }]
      end

      it 'raises an OperationFailure' do
        expect {
          aggregation.to_a
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end

    context 'when the initial response has no results but an active cursor' do
      min_server_fcv '3.2'

      let(:documents) do
        [
            { city: 'a'*6000000 },
            { city: 'b'*6000000 }
        ]
      end

      let(:options) do
        { :use_cursor => true }
      end

      let(:pipeline) do
        [{ '$sample' => { 'size' => 2 } }]
      end

      it 'iterates over the result documents' do
        expect(aggregation.to_a.size).to eq(2)
      end
    end

    context 'when the view has a write concern' do

      let(:collection) do
        authorized_collection.with(write: INVALID_WRITE_CONCERN)
      end

      let(:view) do
        Mongo::Collection::View.new(collection, selector, view_options)
      end

      context 'when the server supports write concern on the aggregate command' do
        min_server_fcv '3.4'

        it 'does not apply the write concern' do
          expect(aggregation.to_a.size).to eq(2)
        end
      end

      context 'when the server does not support write concern on the aggregation command' do
        max_server_version '3.2'

        it 'does not apply the write concern' do
          expect(aggregation.to_a.size).to eq(2)
        end
      end
    end
  end

  describe '#initialize' do

    let(:options) do
      { :cursor => true }
    end

    it 'sets the view' do
      expect(aggregation.view).to eq(view)
    end

    it 'sets the pipeline' do
      expect(aggregation.pipeline).to eq(pipeline)
    end

    it 'sets the options' do
      expect(aggregation.options).to eq(BSON::Document.new(options))
    end

    it 'dups the options' do
      expect(aggregation.options).not_to be(options)
    end
  end

  describe '#explain' do

    it 'executes an explain' do
      expect(aggregation.explain).to_not be_empty
    end

    context 'session id' do
      min_server_fcv '3.6'
      require_topology :replica_set, :sharded

      let(:options) do
        { session: session }
      end

      let(:subscriber) { Mrss::EventSubscriber.new }

      let(:client) do
        authorized_client.tap do |client|
          client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
        end
      end

      let(:session) do
        client.start_session
      end

      let(:view) do
        Mongo::Collection::View.new(client[TEST_COLL], selector, view_options)
      end

      let(:command) do
        aggregation.explain
        subscriber.started_events.find { |c| c.command_name == 'aggregate'}.command
      end

      it 'sends the session id' do
        expect(command['lsid']).to eq(session.session_id)
      end
    end

    context 'when a collation is specified' do

      before do
        authorized_collection.insert_many([ { name: 'bang' }, { name: 'bang' }])
      end

      let(:pipeline) do
        [{ "$match" => { "name" => "BANG" } }]
      end

      let(:result) do
        aggregation.explain['$cursor']['queryPlanner']['collation']['locale']
      end

      context 'when the server selected supports collations' do
        min_server_fcv '3.4'

        shared_examples_for 'applies the collation' do

          context 'when the collation key is a String' do

            let(:options) do
              { 'collation' => { locale: 'en_US', strength: 2 } }
            end

            it 'applies the collation' do
              expect(result).to eq('en_US')
            end
          end

          context 'when the collation key is a Symbol' do

            let(:options) do
              { collation: { locale: 'en_US', strength: 2 } }
            end

            it 'applies the collation' do
              expect(result).to eq('en_US')
            end
          end
        end

        context '4.0-' do
          max_server_version '4.0'

          it_behaves_like 'applies the collation'
        end

        context '4.2+' do
          min_server_fcv '4.2'

          let(:result) do
            aggregation.explain['queryPlanner']['collation']['locale']
          end

          it_behaves_like 'applies the collation'
        end
      end

      context 'when the server selected does not support collations' do
        max_server_version '3.2'

        let(:options) do
          { collation: { locale: 'en_US', strength: 2 } }
        end

        it 'raises an exception' do
          expect {
            result
          }.to raise_exception(Mongo::Error::UnsupportedCollation)
        end

        context 'when a String key is used' do

          let(:options) do
            { 'collation' => { locale: 'en_US', strength: 2 } }
          end

          it 'raises an exception' do
            expect {
              result
            }.to raise_exception(Mongo::Error::UnsupportedCollation)
          end
        end
      end
    end
  end

  describe '#aggregate_spec' do

    context 'when a read preference is given' do

      let(:read_preference) do
        BSON::Document.new({mode: :secondary})
      end

      it 'includes the read preference in the spec' do
        spec = aggregation.send(:aggregate_spec, session, read_preference)
        expect(spec[:read]).to eq(read_preference)
      end
    end

    context 'when allow_disk_use is set' do

      let(:aggregation) do
        described_class.new(view, pipeline, options).allow_disk_use(true)
      end

      it 'includes the option in the spec' do
        expect(aggregation_spec[:selector][:allowDiskUse]).to eq(true)
      end

      context 'when allow_disk_use is specified as an option' do

        let(:options) do
          { :allow_disk_use => true }
        end

        let(:aggregation) do
          described_class.new(view, pipeline, options)
        end

        it 'includes the option in the spec' do
          expect(aggregation_spec[:selector][:allowDiskUse]).to eq(true)
        end

        context 'when #allow_disk_use is also called' do

          let(:options) do
            { :allow_disk_use => true }
          end

          let(:aggregation) do
            described_class.new(view, pipeline, options).allow_disk_use(false)
          end

          it 'overrides the first option with the second' do
            expect(aggregation_spec[:selector][:allowDiskUse]).to eq(false)
          end
        end
      end
    end

    context 'when max_time_ms is an option' do

      let(:options) do
        { :max_time_ms => 100 }
      end

      it 'includes the option in the spec' do
        expect(aggregation_spec[:selector][:maxTimeMS]).to eq(options[:max_time_ms])
      end
    end

    context 'when comment is an option' do

      let(:options) do
        { :comment => 'testing' }
      end

      it 'includes the option in the spec' do
        expect(aggregation_spec[:selector][:comment]).to eq(options[:comment])
      end
    end

    context 'when batch_size is set' do

      context 'when batch_size is set on the view' do

        let(:view_options) do
          { :batch_size => 10 }
        end

        it 'uses the batch_size on the view' do
          expect(aggregation_spec[:selector][:cursor][:batchSize]).to eq(view_options[:batch_size])
        end
      end

      context 'when batch_size is provided in the options' do

        let(:options) do
          { :batch_size => 20 }
        end

        it 'includes the option in the spec' do
          expect(aggregation_spec[:selector][:cursor][:batchSize]).to eq(options[:batch_size])
        end

        context 'when  batch_size is also set on the view' do

          let(:view_options) do
            { :batch_size => 10 }
          end

          it 'overrides the view batch_size with the option batch_size' do
            expect(aggregation_spec[:selector][:cursor][:batchSize]).to eq(options[:batch_size])
          end
        end
      end
    end

    context 'when a hint is specified' do

      let(:options) do
        { 'hint' => { 'y' => 1 } }
      end

      it 'includes the option in the spec' do
        expect(aggregation_spec[:selector][:hint]).to eq(options['hint'])
      end
    end

    context 'when use_cursor is set' do

      context 'when use_cursor is true' do

        context 'when batch_size is set' do

          let(:options) do
            { :use_cursor => true,
              :batch_size => 10
            }
          end

          it 'sets a batch size document in the spec' do
            expect(aggregation_spec[:selector][:cursor][:batchSize]).to eq(options[:batch_size])
          end
        end

        context 'when batch_size is not set' do

          let(:options) do
            { :use_cursor => true }
          end

          it 'sets an empty document in the spec' do
            expect(aggregation_spec[:selector][:cursor]).to eq({})
          end
        end

      end

      context 'when use_cursor is false' do

        let(:options) do
          { :use_cursor => false }
        end

        context 'when batch_size is set' do

          it 'does not set the cursor option in the spec' do
            expect(aggregation_spec[:selector][:cursor]).to be_nil
          end
        end
      end
    end
  end

  context 'when the aggregation has a collation defined' do

    before do
      authorized_collection.insert_many([ { name: 'bang' }, { name: 'bang' }])
    end

    let(:pipeline) do
      [{ "$match" => { "name" => "BANG" } }]
    end

    let(:options) do
      { collation: { locale: 'en_US', strength: 2 } }
    end

    let(:result) do
      aggregation.collect { |doc| doc['name']}
    end

    context 'when the server selected supports collations' do
      min_server_fcv '3.4'

      it 'applies the collation' do
        expect(result).to eq(['bang', 'bang'])
      end
    end

    context 'when the server selected does not support collations' do
      max_server_version '3.2'

      it 'raises an exception' do
        expect {
          result
        }.to raise_exception(Mongo::Error::UnsupportedCollation)
      end

      context 'when a String key is used' do

        let(:options) do
          { 'collation' => { locale: 'en_US', strength: 2 } }
        end

        it 'raises an exception' do
          expect {
            result
          }.to raise_exception(Mongo::Error::UnsupportedCollation)
        end
      end
    end
  end

  context 'when $out is in the pipeline' do
    [['$out', 'string'], [:$out, 'symbol']].each do |op, type|
      context "when #{op} is a #{type}" do
        let(:pipeline) do
          [{
               "$group" => {
                   "_id" => "$city",
                   "totalpop" => { "$sum" => "$pop" }
               }
           },
           {
               op => 'output_collection'
           }
          ]
        end

        before do
          authorized_client['output_collection'].delete_many
        end

        let(:features) do
          double()
        end

        let(:server) do
          double().tap do |server|
            allow(server).to receive(:features).and_return(features)
          end
        end

        context 'when the view has a write concern' do

          let(:collection) do
            authorized_collection.with(write: INVALID_WRITE_CONCERN)
          end

          let(:view) do
            Mongo::Collection::View.new(collection, selector, view_options)
          end

          context 'when the server supports write concern on the aggregate command' do
            min_server_fcv '3.4'

            it 'uses the write concern' do
              expect {
                aggregation.to_a
              }.to raise_exception(Mongo::Error::OperationFailure)
            end
          end

          context 'when the server does not support write concern on the aggregation command' do
            max_server_version '3.2'

            let(:documents) do
              [
                { city: "Berlin", pop: 18913, neighborhood: "Kreuzberg" },
                { city: "Berlin", pop: 84143, neighborhood: "Mitte" },
                { city: "New York", pop: 40270, neighborhood: "Brooklyn" }
              ]
            end

            before do
              authorized_collection.insert_many(documents)
              aggregation.to_a
            end

            it 'does not apply the write concern' do
              expect(authorized_client['output_collection'].find.count).to eq(2)
            end
          end
        end
      end
    end
  end

  context "when there is a filter on the view" do

    context "when broken_view_aggregate is turned off" do
      config_override :broken_view_aggregate, false

      let(:documents) do
        [
          { city: "Berlin", pop: 18913, neighborhood: "Kreuzberg" },
          { city: "Berlin", pop: 84143, neighborhood: "Mitte" },
          { city: "New York", pop: 40270, neighborhood: "Brooklyn" }
        ]
      end

      let(:pipeline) do
        [{
          "$project" => {
            city: 1
          }
        }]
      end

      let(:view) do
        authorized_collection.find(city: "Berlin")
      end

      before do
        authorized_collection.delete_many
        authorized_collection.insert_many(documents)
      end

      it "uses the filter on the view" do
        expect(aggregation.to_a.length).to eq(2)
      end

      it "adds a match stage" do
        expect(aggregation.pipeline.length).to eq(2)
        expect(aggregation.pipeline.first).to eq({ :$match => { "city" => "Berlin" } })
      end
    end

    context "when broken_view_aggregate is turned on" do
      config_override :broken_view_aggregate, true

      let(:documents) do
        [
          { city: "Berlin", pop: 18913, neighborhood: "Kreuzberg" },
          { city: "Berlin", pop: 84143, neighborhood: "Mitte" },
          { city: "New York", pop: 40270, neighborhood: "Brooklyn" }
        ]
      end

      let(:pipeline) do
        [{
          "$project" => {
            city: 1
          }
        }]
      end

      let(:view) do
        authorized_collection.find(city: "Berlin")
      end

      before do
        authorized_collection.delete_many
        authorized_collection.insert_many(documents)
      end

      it "ignores the view filter" do
        expect(aggregation.to_a.length).to eq(3)
      end

      it "does not add a match stage" do
        expect(aggregation.pipeline.length).to eq(1)
        expect(aggregation.pipeline).to eq([ { "$project" => { city: 1 } } ])
      end
    end
  end

  context "when there is no filter on the view" do

    with_config_values :broken_view_aggregate, true, false do

      let(:documents) do
        [
          { city: "Berlin", pop: 18913, neighborhood: "Kreuzberg" },
          { city: "Berlin", pop: 84143, neighborhood: "Mitte" },
          { city: "New York", pop: 40270, neighborhood: "Brooklyn" }
        ]
      end

      let(:pipeline) do
        [{
          "$project" => {
            city: 1
          }
        }]
      end

      let(:view) do
        authorized_collection.find
      end

      before do
        authorized_collection.delete_many
        authorized_collection.insert_many(documents)
      end

      it "ignores the view filter" do
        expect(aggregation.to_a.length).to eq(3)
      end

      it "does not add a match stage" do
        expect(aggregation.pipeline.length).to eq(1)
        expect(aggregation.pipeline).to eq([ { "$project" => { city: 1 } } ])
      end
    end
  end
end
