# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Collection::View::MapReduce do
  clean_slate_on_evergreen

  let(:map) do
  %Q{
  function() {
    emit(this.name, { population: this.population });
  }}
  end

  let(:reduce) do
    %Q{
    function(key, values) {
      var result = { population: 0 };
      values.forEach(function(value) {
        result.population += value.population;
      });
      return result;
    }}
  end

  let(:documents) do
    [
      { name: 'Berlin', population: 3000000 },
      { name: 'London', population: 9000000 }
    ]
  end

  let(:selector) do
    {}
  end

  let(:view_options) do
    {}
  end

  let(:view) do
    authorized_client.cluster.servers.map do |server|
      server.pool.ready
    end

    Mongo::Collection::View.new(authorized_collection, selector, view_options)
  end

  let(:options) do
    {}
  end

  let(:map_reduce_spec) do
    map_reduce.send(:map_reduce_spec, double('session'))
  end

  before do
    authorized_collection.delete_many
    authorized_collection.insert_many(documents)
  end

  let(:map_reduce) do
    described_class.new(view, map, reduce, options)
  end

  describe '#initialize' do
    it 'warns of deprecation' do
      Mongo::Logger.logger.should receive(:warn).with('MONGODB | The map_reduce operation is deprecated, please use the aggregation pipeline instead')

      map_reduce
    end
  end

  describe '#map_function' do

    it 'returns the map function' do
      expect(map_reduce.map_function).to eq(map)
    end
  end

  describe '#reduce_function' do

    it 'returns the reduce function' do
      expect(map_reduce.reduce_function).to eq(reduce)
    end
  end

  describe '#map' do

    let(:results) do
      map_reduce.map do |document|
        document
      end
    end

    it 'calls the Enumerable method' do
      expect(results.sort_by { |d| d['_id'] }).to eq(map_reduce.to_a.sort_by { |d| d['_id'] })
    end
  end

  describe '#reduce' do

    let(:results) do
      map_reduce.reduce(0) { |sum, doc| sum + doc['value']['population'] }
    end

    it 'calls the Enumerable method' do
      expect(results).to eq(12000000)
    end
  end

  describe '#each' do

    context 'when no options are provided' do

      it 'iterates over the documents in the result' do
        map_reduce.each do |document|
          expect(document[:value]).to_not be_nil
        end
      end
    end

    context 'when provided a session' do

      let(:options) do
        { session: session }
      end

      let(:operation) do
        map_reduce.to_a
      end

      let(:client) do
        authorized_client
      end

      it_behaves_like 'an operation using a session'
    end

    context 'when out is in the options' do

      before do
        authorized_client['output_collection'].delete_many
      end

      context 'when out is a string' do

        let(:options) do
          { :out => 'output_collection' }
        end

        it 'iterates over the documents in the result' do
          map_reduce.each do |document|
            expect(document[:value]).to_not be_nil
          end
        end
      end

      context 'when out is a document' do

        let(:options) do
          { :out => {  replace: 'output_collection' } }
        end

        it 'iterates over the documents in the result' do
          map_reduce.each do |document|
            expect(document[:value]).to_not be_nil
          end
        end
      end
    end

    context 'when out is inline' do

      let(:new_map_reduce) do
        map_reduce.out(inline: 1)
      end

      it 'iterates over the documents in the result' do
        new_map_reduce.each do |document|
          expect(document[:value]).to_not be_nil
        end
      end
    end

    context 'when out is a collection' do

      before do
        authorized_client['output_collection'].delete_many
      end

      context 'when #each is called without a block' do

        let(:new_map_reduce) do
          map_reduce.out(replace: 'output_collection')
        end

        before do
          new_map_reduce.each
        end

        it 'executes the map reduce' do
          expect(new_map_reduce.to_a.sort_by { |d| d['_id'] }).to eq(map_reduce.to_a.sort_by { |d| d['_id'] })
        end
      end

      context 'when the option is to replace' do

        let(:new_map_reduce) do
          map_reduce.out(replace: 'output_collection')
        end

        it 'iterates over the documents in the result' do
          new_map_reduce.each do |document|
            expect(document[:value]).to_not be_nil
          end
        end

        it 'fetches the results from the collection' do
          expect(new_map_reduce.count).to eq(2)
        end

        context 'when provided a session' do

          let(:options) do
            { session: session }
          end

          let(:operation) do
            new_map_reduce.to_a
          end

          let(:client) do
            authorized_client
          end

          it_behaves_like 'an operation using a session'
        end

        context 'when the output collection is iterated' do
          min_server_fcv '3.6'
          require_topology :replica_set, :sharded

          let(:options) do
            { session: session }
          end

          let(:session) do
            client.start_session
          end

          let(:view) do
            Mongo::Collection::View.new(client[TEST_COLL], selector, view_options)
          end

          let(:subscriber) { Mrss::EventSubscriber.new }

          let(:client) do
            authorized_client.tap do |client|
              client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
            end
          end

          let(:find_command) do
            subscriber.started_events[-1].command
          end

          before do
            begin; client[TEST_COLL].create; rescue; end
            begin; client.use('another-db')[TEST_COLL].create; rescue; end
          end

          it 'uses the session when iterating over the output collection' do
            new_map_reduce.to_a
            expect(find_command["lsid"]).to eq(BSON::Document.new(session.session_id))
          end
        end

        context 'when another db is specified' do
          min_server_fcv '3.6'
          require_topology :single, :replica_set
          require_no_auth

          let(:new_map_reduce) do
            map_reduce.out(db: 'another-db', replace: 'output_collection')
          end

          it 'iterates over the documents in the result' do
            new_map_reduce.each do |document|
              expect(document[:value]).to_not be_nil
            end
          end

          it 'fetches the results from the collection'  do
            expect(new_map_reduce.count).to eq(2)
          end
        end
      end

      context 'when the option is to merge' do

        let(:new_map_reduce) do
          map_reduce.out(merge: 'output_collection')
        end

        it 'iterates over the documents in the result' do
          new_map_reduce.each do |document|
            expect(document[:value]).to_not be_nil
          end
        end

        it 'fetches the results from the collection' do
          expect(new_map_reduce.count).to eq(2)
        end

        context 'when another db is specified' do
          min_server_fcv '3.0'
          require_topology :single, :replica_set
          require_no_auth

          let(:new_map_reduce) do
            map_reduce.out(db: 'another-db', merge: 'output_collection')
          end

          it 'iterates over the documents in the result' do
            new_map_reduce.each do |document|
              expect(document[:value]).to_not be_nil
            end
          end

          it 'fetches the results from the collection' do
            expect(new_map_reduce.count).to eq(2)
          end
        end
      end

      context 'when the option is to reduce' do

        let(:new_map_reduce) do
          map_reduce.out(reduce: 'output_collection')
        end

        it 'iterates over the documents in the result' do
          new_map_reduce.each do |document|
            expect(document[:value]).to_not be_nil
          end
        end

        it 'fetches the results from the collection' do
          expect(new_map_reduce.count).to eq(2)
        end

        context 'when another db is specified' do
          min_server_fcv '3.0'
          require_topology :single, :replica_set
          require_no_auth

          let(:new_map_reduce) do
            map_reduce.out(db: 'another-db', reduce: 'output_collection')
          end

          it 'iterates over the documents in the result' do
            new_map_reduce.each do |document|
              expect(document[:value]).to_not be_nil
            end
          end

          it 'fetches the results from the collection' do
            expect(new_map_reduce.count).to eq(2)
          end
        end
      end

      context 'when the option is a collection name' do

        let(:new_map_reduce) do
          map_reduce.out('output_collection')
        end

        it 'fetches the results from the collection' do
          expect(new_map_reduce.count).to eq(2)
        end
      end
    end

    context 'when the view has a selector' do

      context 'when the selector is basic' do

        let(:selector) do
          { 'name' => 'Berlin' }
        end

        it 'applies the selector to the map/reduce' do
          map_reduce.each do |document|
            expect(document[:_id]).to eq('Berlin')
          end
        end

        it 'includes the selector in the operation spec' do
          expect(map_reduce_spec[:selector][:query]).to eq(selector)
        end
      end

      context 'when the selector is advanced' do

        let(:selector) do
          { :$query => { 'name' => 'Berlin' }}
        end

        it 'applies the selector to the map/reduce' do
          map_reduce.each do |document|
            expect(document[:_id]).to eq('Berlin')
          end
        end

        it 'includes the selector in the operation spec' do
          expect(map_reduce_spec[:selector][:query]).to eq(selector[:$query])
        end
      end
    end

    context 'when the view has a limit' do

      let(:view_options) do
        { limit: 1 }
      end

      it 'applies the limit to the map/reduce' do
        map_reduce.each do |document|
          expect(document[:_id]).to eq('Berlin')
        end
      end
    end
  end

  describe '#execute' do

    context 'when output is to a collection' do

      let(:options) do
        { out: 'output_collection' }
      end

      let!(:result) do
        map_reduce.execute
      end

      it 'executes the map reduce' do
        expect(authorized_client['output_collection'].count).to eq(2)
      end

      it 'returns a result object' do
        expect(result).to be_a(Mongo::Operation::Result)
      end
    end

    context 'when there is no output' do

      let(:result) do
        map_reduce.execute
      end

      it 'executes the map reduce' do
        expect(result.documents.size).to eq(2)
      end

      it 'returns a result object' do
        expect(result).to be_a(Mongo::Operation::Result)
      end
    end

    context 'when a session is provided' do

      let(:session) do
        authorized_client.start_session
      end

      let(:options) do
        { session: session }
      end

      let(:operation) do
        map_reduce.execute
      end

      let(:failed_operation) do
        described_class.new(view, '$invalid', reduce, options).execute
      end

      let(:client) do
        authorized_client
      end

      it_behaves_like 'an operation using a session'
      it_behaves_like 'a failed operation using a session'
    end
  end

  describe '#finalize' do

    let(:finalize) do
    %Q{
    function(key, value) {
      value.testing = test;
      return value;
    }}
    end

    let(:new_map_reduce) do
      map_reduce.finalize(finalize)
    end

    it 'sets the finalize function' do
      expect(new_map_reduce.finalize).to eq(finalize)
    end

    it 'includes the finalize function in the operation spec' do
      expect(new_map_reduce.send(:map_reduce_spec, double('session'))[:selector][:finalize]).to eq(finalize)
    end
  end

  describe '#js_mode' do

    let(:new_map_reduce) do
      map_reduce.js_mode(true)
    end

    it 'sets the js mode value' do
      expect(new_map_reduce.js_mode).to be true
    end

    it 'includes the js mode value in the operation spec' do
      expect(new_map_reduce.send(:map_reduce_spec, double('session'))[:selector][:jsMode]).to be(true)
    end
  end

  describe '#out' do

    let(:location) do
      { 'replace' => 'testing' }
    end

    let(:new_map_reduce) do
      map_reduce.out(location)
    end

    it 'sets the out value' do
      expect(new_map_reduce.out).to eq(location)
    end

    it 'includes the out value in the operation spec' do
      expect(new_map_reduce.send(:map_reduce_spec, double('session'))[:selector][:out]).to eq(location)
    end

    context 'when out is not defined' do

      it 'defaults to inline' do
        expect(map_reduce_spec[:selector][:out]).to eq('inline' => 1)
      end
    end

    context 'when out is specified in the options' do

      let(:location) do
        { 'replace' => 'testing' }
      end

      let(:options) do
        { :out => location }
      end

      it 'sets the out value' do
        expect(map_reduce.out).to eq(location)
      end

      it 'includes the out value in the operation spec' do
        expect(map_reduce_spec[:selector][:out]).to eq(location)
      end
    end

    context 'when out is not inline' do

      let(:location) do
        { 'replace' => 'testing' }
      end

      let(:options) do
        { :out => location }
      end

      it 'does not allow the operation on a secondary' do
        expect(map_reduce.send(:secondary_ok?)).to be false
      end

      context 'when the server is not valid for writing' do
        clean_slate
        require_warning_clean
        require_no_linting

        before do
          stop_monitoring(authorized_client)
        end

        it 'reroutes the operation to a primary' do
          RSpec::Mocks.with_temporary_scope do
            allow(map_reduce).to receive(:valid_server?).and_return(false)
            expect(Mongo::Logger.logger).to receive(:warn).once do |msg|
              expect(msg).to include('Rerouting the MapReduce operation to the primary server')
            end
            map_reduce.to_a
          end
        end

        context 'when the view has a write concern' do

          let(:collection) do
            authorized_collection.with(write: INVALID_WRITE_CONCERN)
          end

          let(:view) do
            authorized_client.cluster.servers.map do |server|
              server.pool.ready
            end

            Mongo::Collection::View.new(collection, selector, view_options)
          end

          shared_examples_for 'map reduce that writes accepting write concern' do

            context 'when the server supports write concern on the mapReduce command' do
              min_server_fcv '3.4'
              require_topology :single

              it 'uses the write concern' do
                expect {
                  map_reduce.to_a
                }.to raise_exception(Mongo::Error::OperationFailure)
              end
            end

            context 'when the server does not support write concern on the mapReduce command' do
              max_server_version '3.2'

              it 'does not apply the write concern' do
                expect(map_reduce.to_a.size).to eq(2)
              end
            end
          end

          context 'when out is a String' do

            let(:options) do
              { :out => 'new-collection' }
            end

            it_behaves_like 'map reduce that writes accepting write concern'
          end

          context 'when out is a document and not inline' do

            let(:options) do
              { :out => { merge: 'exisiting-collection' } }
            end

            it_behaves_like 'map reduce that writes accepting write concern'
          end

          context 'when out is a document but inline is specified' do

            let(:options) do
              { :out => { inline: 1 } }
            end

            it 'does not use the write concern' do
              expect(map_reduce.to_a.size).to eq(2)
            end
          end
        end
      end

      context 'when the server is a valid for writing' do
        clean_slate
        require_warning_clean
        require_no_linting

        before do
          stop_monitoring(authorized_client)
        end

        it 'does not reroute the operation to a primary' do
          # We produce a deprecation warning, but there shouldn't be
          # the reroute warning.
          expect(Mongo::Logger.logger).to receive(:warn).once do |msg|
            expect(msg).not_to include('Rerouting the MapReduce operation to the primary server')
          end

          map_reduce.to_a
        end
      end
    end
  end

  describe '#scope' do

    let(:object) do
      { 'value' => 'testing' }
    end

    let(:new_map_reduce) do
      map_reduce.scope(object)
    end

    it 'sets the scope object' do
      expect(new_map_reduce.scope).to eq(object)
    end

    it 'includes the scope object in the operation spec' do
      expect(new_map_reduce.send(:map_reduce_spec, double('session'))[:selector][:scope]).to eq(object)
    end
  end

  describe '#verbose' do

    let(:verbose) do
      false
    end

    let(:new_map_reduce) do
      map_reduce.verbose(verbose)
    end

    it 'sets the verbose value' do
      expect(new_map_reduce.verbose).to be(false)
    end

    it 'includes the verbose option in the operation spec' do
      expect(new_map_reduce.send(:map_reduce_spec, double('session'))[:selector][:verbose]).to eq(verbose)
    end
  end

  context 'when limit is set on the view' do

    let(:limit) do
      3
    end

    let(:view_options) do
      { limit: limit }
    end

    it 'includes the limit in the operation spec' do
      expect(map_reduce_spec[:selector][:limit]).to be(limit)
    end
  end

  context 'when sort is set on the view' do

    let(:sort) do
      { name: -1 }
    end

    let(:view_options) do
      { sort: sort }
    end

    it 'includes the sort object in the operation spec' do
      expect(map_reduce_spec[:selector][:sort][:name]).to eq(sort[:name])
    end
  end

  context 'when the collection has a read preference' do

    let(:read_preference) do
      {mode: :secondary}
    end

    it 'includes the read preference in the spec' do
      allow(authorized_collection).to receive(:read_preference).and_return(read_preference)
      expect(map_reduce_spec[:read]).to eq(read_preference)
    end
  end

  context 'when collation is specified' do

    let(:map) do
      %Q{
         function() {
           emit(this.name, 1);
        }}
    end

    let(:reduce) do
      %Q{
         function(key, values) {
           return Array.sum(values);
        }}
    end

    before do
      authorized_collection.insert_many([ { name: 'bang' }, { name: 'bang' }])
    end

    let(:selector) do
      { name: 'BANG' }
    end

    context 'when the server selected supports collations' do
      min_server_fcv '3.4'

      context 'when the collation key is a String' do

        let(:options) do
          { 'collation' => { locale: 'en_US', strength: 2 } }
        end

        it 'applies the collation' do
          expect(map_reduce.first['value']).to eq(2)
        end
      end

      context 'when the collation key is a Symbol' do

        let(:options) do
          { collation: { locale: 'en_US', strength: 2 } }
        end

        it 'applies the collation' do
          expect(map_reduce.first['value']).to eq(2)
        end
      end
    end

    context 'when the server selected does not support collations' do
      max_server_version '3.2'

      context 'when the map reduce has collation specified in its options' do

        let(:options) do
          { collation: { locale: 'en_US', strength: 2 } }
        end

        it 'raises an exception' do
          expect {
            map_reduce.to_a
          }.to raise_exception(Mongo::Error::UnsupportedCollation)
        end

        context 'when a String key is used' do

          let(:options) do
            { 'collation' => { locale: 'en_US', strength: 2 } }
          end

          it 'raises an exception' do
            expect {
              map_reduce.to_a
            }.to raise_exception(Mongo::Error::UnsupportedCollation)
          end
        end
      end

      context 'when the view has collation specified in its options' do

        let(:view_options) do
          { collation: { locale: 'en_US', strength: 2 } }
        end

        it 'raises an exception' do
          expect {
            map_reduce.to_a
          }.to raise_exception(Mongo::Error::UnsupportedCollation)
        end

        context 'when a String key is used' do

          let(:options) do
            { 'collation' => { locale: 'en_US', strength: 2 } }
          end

          it 'raises an exception' do
            expect {
              map_reduce.to_a
            }.to raise_exception(Mongo::Error::UnsupportedCollation)
          end
        end
      end
    end
  end

  describe '#map_reduce_spec' do
    context 'when read preference is given' do
      let(:view_options) do
        { read: {mode: :secondary} }
      end

      context 'selector' do
        # For compatibility with released versions of Mongoid, this method
        # must return read preference under the :read key.
        it 'contains read preference' do
          map_reduce_spec[:selector][:read].should == {'mode' => :secondary}
        end
      end
    end
  end
end
