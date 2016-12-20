require 'spec_helper'

describe Mongo::Collection::View::Readable do

  let(:selector) do
    {}
  end

  let(:options) do
    {}
  end

  let(:view) do
    Mongo::Collection::View.new(authorized_collection, selector, options)
  end

  after do
    authorized_collection.delete_many
  end

  shared_examples_for 'a read concern aware operation' do

    context 'when a read concern is provided', if: find_command_enabled? do

      let(:new_view) do
        Mongo::Collection::View.new(new_collection, selector, options)
      end

      context 'when the read concern is valid' do

        let(:new_collection) do
          authorized_collection.with(read_concern: { level: 'local' })
        end

        it 'sends the read concern' do
          expect { result }.to_not raise_error
        end
      end

      context 'when the read concern is not valid' do

        let(:new_collection) do
          authorized_collection.with(read_concern: { level: 'na' })
        end

        it 'raises an exception' do
          expect {
            result
          }.to raise_error(Mongo::Error::OperationFailure)
        end
      end
    end
  end

  describe '#allow_partial_results' do

    let(:new_view) do
      view.allow_partial_results
    end

    it 'sets the flag' do
      expect(new_view.options[:allow_partial_results]).to be true
    end

    it 'returns a new View' do
      expect(new_view).not_to be(view)
    end
  end

  describe '#aggregate' do

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
      authorized_collection.insert_many(documents)
    end

    let(:aggregation) do
      view.aggregate(pipeline)
    end

    context 'when incorporating read concern' do

      let(:result) do
        new_view.aggregate(pipeline, options).to_a
      end

      it_behaves_like 'a read concern aware operation'
    end

    context 'when not iterating the aggregation' do

      it 'returns the aggregation object' do
        expect(aggregation).to be_a(Mongo::Collection::View::Aggregation)
      end
    end

    context 'when iterating the aggregation' do

      it 'yields to each document' do
        aggregation.each do |doc|
          expect(doc[:totalpop]).to_not be_nil
        end
      end
    end

    context 'when options are specified' do

      let(:agg_options) do
        { :max_time_ms => 500 }
      end

      let(:aggregation) do
        view.aggregate(pipeline, agg_options)
      end

      it 'passes the option to the Aggregation object' do
        expect(aggregation.options[:max_time_ms]).to eq(agg_options[:max_time_ms])
      end
    end
  end

  describe '#map_reduce' do

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

    before do
      authorized_collection.insert_many(documents)
    end

    let(:map_reduce) do
      view.map_reduce(map, reduce)
    end

    context 'when incorporating read concern' do

      let(:result) do
        new_view.map_reduce(map, reduce, options).to_a
      end

      it_behaves_like 'a read concern aware operation'
    end

    context 'when not iterating the map/reduce' do

      it 'returns the map/reduce object' do
        expect(map_reduce).to be_a(Mongo::Collection::View::MapReduce)
      end
    end

    context 'when iterating the map/reduce' do

      it 'yields to each document' do
        map_reduce.each do |doc|
          expect(doc[:_id]).to_not be_nil
        end
      end
    end
  end

  describe '#batch_size' do

    let(:options) do
      { :batch_size => 13 }
    end

    context 'when a batch size is specified' do

      let(:new_batch_size) do
        15
      end

      it 'sets the batch size' do
        new_view = view.batch_size(new_batch_size)
        expect(new_view.batch_size).to eq(new_batch_size)
      end

      it 'returns a new View' do
        expect(view.batch_size(new_batch_size)).not_to be(view)
      end
    end

    context 'when a batch size is not specified' do

      it 'returns the batch_size' do
        expect(view.batch_size).to eq(options[:batch_size])
      end
    end
  end

  describe '#comment' do

    let(:options) do
      { :comment => 'test1' }
    end

    context 'when a comment is specified' do

      let(:new_comment) do
        'test2'
      end

      it 'sets the comment' do
        new_view = view.comment(new_comment)
        expect(new_view.comment).to eq(new_comment)
      end

      it 'returns a new View' do
        expect(view.comment(new_comment)).not_to be(view)
      end
    end

    context 'when a comment is not specified' do

      it 'returns the comment' do
        expect(view.comment).to eq(options[:comment])
      end
    end
  end

  describe '#count' do

    let(:documents) do
      (1..10).map{ |i| { field: "test#{i}" }}
    end

    before do
      authorized_collection.insert_many(documents)
    end

    after do
      authorized_collection.delete_many
    end

    let(:result) do
      view.count(options)
    end

    context 'when incorporating read concern' do

      let(:result) do
        new_view.count(options)
      end

      it_behaves_like 'a read concern aware operation'
    end

    context 'when a selector is provided' do

      let(:selector) do
        { field: 'test1' }
      end

      it 'returns the count of matching documents' do
        expect(view.count).to eq(1)
      end

      it 'returns an integer' do
        expect(view.count).to be_a(Integer)
      end
    end

    context 'when no selector is provided' do

      it 'returns the count of matching documents' do
        expect(view.count).to eq(10)
      end
    end

    it 'takes a read preference option' do
      expect(view.count(read: { mode: :secondary })).to eq(10)
    end

    context 'when a read preference is set on the view', unless: sharded? do

      let(:client) do
        # Set a timeout otherwise, the test will hang for 30 seconds.
        authorized_client.with(server_selection_timeout: 1)
      end

      let(:collection) do
        client[authorized_collection.name]
      end

      before do
        allow(client.cluster).to receive(:single?).and_return(false)
      end

      let(:view) do
        Mongo::Collection::View.new(collection, selector, options)
      end

      let(:view_with_read_pref) do
        view.read(:mode => :secondary, :tag_sets => [{ 'non' => 'existent' }])
      end

      let(:result) do
        view_with_read_pref.count
      end

      it 'uses the read preference setting on the view' do
        expect {
          result
        }.to raise_exception(Mongo::Error::NoServerAvailable)
      end
    end

    context 'when the collection has a read preference set' do

      after do
        client.close
      end

      let(:client) do
        # Set a timeout in case the collection read_preference does get used.
        # Otherwise, the test will hang for 30 seconds.
        authorized_client.with(server_selection_timeout: 1)
      end

      let(:read_preference) do
        { :mode => :secondary, :tag_sets => [{ 'non' => 'existent' }] }
      end

      let(:collection) do
        client[authorized_collection.name, read: read_preference]
      end

      let(:view) do
        Mongo::Collection::View.new(collection, selector, options)
      end

      context 'when a read preference argument is provided' do

        let(:result) do
          view.count(read: { mode: :primary })
        end

        it 'uses the read preference passed to the method' do
          expect(result).to eq(10)
        end
      end

      context 'when a read preference is set on the view' do

        let(:view_with_read_pref) do
          view.read(mode: :primary)
        end

        let(:result) do
          view_with_read_pref.count
        end

        it 'uses the read preference of the view' do
          expect(result).to eq(10)
        end
      end

      context 'when no read preference argument is provided', unless: sharded? do

        before do
          allow(view.collection.client.cluster).to receive(:single?).and_return(false)
        end

        let(:result) do
          view.count
        end

        it 'uses the read preference of the collection' do
          expect {
            result
          }.to raise_exception(Mongo::Error::NoServerAvailable)
        end
      end

      context 'when the collection does not have a read preference set', unless: sharded? do

        after do
          client.close
        end

        let(:client) do
          authorized_client.with(server_selection_timeout: 1)
        end

        before do
          allow(view.collection.client.cluster).to receive(:single?).and_return(false)
        end

        let(:collection) do
          client[authorized_collection.name]
        end

        let(:view) do
          Mongo::Collection::View.new(collection, selector, options)
        end

        let(:result) do
          read_preference = { :mode => :secondary, :tag_sets => [{ 'non' => 'existent' }] }
          view.count(read: read_preference)
        end

        it 'uses the read preference passed to the method' do
          expect {
            result
          }.to raise_exception(Mongo::Error::NoServerAvailable)
        end
      end

      context 'when a read preference is set on the view' do

        let(:view_with_read_pref) do
          view.read(:mode => :primary)
        end

        let(:result) do
          view_with_read_pref.count
        end

        it 'uses the read preference passed to the method' do
          expect(result).to eq(10)
        end
      end
    end

    it 'takes a max_time_ms option', if: write_command_enabled? do
      expect {
        view.count(max_time_ms: 0.1)
      }.to raise_error(Mongo::Error::OperationFailure)
    end

    it 'sets the max_time_ms option on the command', if: write_command_enabled? do
      expect(view.count(max_time_ms: 100)).to eq(10)
    end

    context 'when a collation is specified' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        view.count
      end

      before do
        authorized_collection.insert_one(name: 'bang')
      end

      let(:options) do
        { collation: { locale: 'en_US', strength: 2 } }
      end

      context 'when the server selected supports collations', if: collation_enabled? do

        it 'applies the collation to the count' do
          expect(result).to eq(1)
        end
      end

      context 'when the server selected does not support collations', unless: collation_enabled? do

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

    context 'when a collation is specified in the method options' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        view.count(count_options)
      end

      before do
        authorized_collection.insert_one(name: 'bang')
      end

      let(:count_options) do
        { collation: { locale: 'en_US', strength: 2 } }
      end

      context 'when the server selected supports collations', if: collation_enabled? do

        it 'applies the collation to the count' do
          expect(result).to eq(1)
        end
      end

      context 'when the server selected does not support collations', unless: collation_enabled? do

        it 'raises an exception' do
          expect {
            result
          }.to raise_exception(Mongo::Error::UnsupportedCollation)
        end

        context 'when a String key is used' do

          let(:count_options) do
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

  describe '#distinct' do

    context 'when incorporating read concern' do

      let(:result) do
        new_view.distinct(:field, options)
      end

      it_behaves_like 'a read concern aware operation'
    end

    context 'when a selector is provided' do

      let(:selector) do
        { field: 'test' }
      end

      let(:documents) do
        (1..3).map{ |i| { field: "test" }}
      end

      before do
        authorized_collection.insert_many(documents)
      end

      context 'when the field is a symbol' do

        let(:distinct) do
          view.distinct(:field)
        end

        it 'returns the distinct values' do
          expect(distinct).to eq([ 'test' ])
        end
      end

      context 'when the field is a string' do

        let(:distinct) do
          view.distinct('field')
        end

        it 'returns the distinct values' do
          expect(distinct).to eq([ 'test' ])
        end
      end

      context 'when the field is nil' do

        let(:distinct) do
          view.distinct(nil)
        end

        it 'returns an empty array' do
          expect(distinct).to be_empty
        end
      end

      context 'when the field does not exist' do

        let(:distinct) do
          view.distinct(:doesnotexist)
        end

        it 'returns an empty array' do
          expect(distinct).to be_empty
        end
      end
    end

    context 'when no selector is provided' do

      let(:documents) do
        (1..3).map{ |i| { field: "test#{i}" }}
      end

      before do
        authorized_collection.insert_many(documents)
      end

      context 'when the field is a symbol' do

        let(:distinct) do
          view.distinct(:field)
        end

        it 'returns the distinct values' do
          expect(distinct.sort).to eq([ 'test1', 'test2', 'test3' ])
        end
      end

      context 'when the field is a string' do

        let(:distinct) do
          view.distinct('field')
        end

        it 'returns the distinct values' do
          expect(distinct.sort).to eq([ 'test1', 'test2', 'test3' ])
        end
      end

      context 'when the field is nil' do

        let(:distinct) do
          view.distinct(nil)
        end

        it 'returns an empty array' do
          expect(distinct).to be_empty
        end
      end
    end

    context 'when a read preference is set on the view', unless: sharded? do

      let(:client) do
        # Set a timeout otherwise, the test will hang for 30 seconds.
        authorized_client.with(server_selection_timeout: 1)
      end

      let(:collection) do
        client[authorized_collection.name]
      end

      before do
        allow(client.cluster).to receive(:single?).and_return(false)
      end

      let(:view) do
        Mongo::Collection::View.new(collection, selector, options)
      end

      let(:view_with_read_pref) do
        view.read(:mode => :secondary, :tag_sets => [{ 'non' => 'existent' }])
      end

      let(:result) do
        view_with_read_pref.distinct(:field)
      end

      it 'uses the read preference setting on the view' do
        expect {
          result
        }.to raise_exception(Mongo::Error::NoServerAvailable)
      end
    end

    context 'when the collection has a read preference set' do

      let(:documents) do
        (1..3).map{ |i| { field: "test#{i}" }}
      end

      before do
        authorized_collection.insert_many(documents)
      end

      after do
        client.close
      end

      let(:client) do
        # Set a timeout in case the collection read_preference does get used.
        # Otherwise, the test will hang for 30 seconds.
        authorized_client.with(server_selection_timeout: 1)
      end

      let(:read_preference) do
        { :mode => :secondary, :tag_sets => [{ 'non' => 'existent' }] }
      end

      let(:collection) do
        client[authorized_collection.name, read: read_preference]
      end

      let(:view) do
        Mongo::Collection::View.new(collection, selector, options)
      end

      context 'when a read preference argument is provided' do

        let(:distinct) do
          view.distinct(:field, read: { mode: :primary })
        end

        it 'uses the read preference passed to the method' do
          expect(distinct.sort).to eq([ 'test1', 'test2', 'test3' ])
        end
      end

      context 'when no read preference argument is provided', unless: sharded? do

        before do
          allow(view.collection.client.cluster).to receive(:single?).and_return(false)
        end

        let(:distinct) do
          view.distinct(:field)
        end

        it 'uses the read preference of the collection' do
          expect {
            distinct
          }.to raise_exception(Mongo::Error::NoServerAvailable)
        end
      end

      context 'when the collection does not have a read preference set', unless: sharded? do

        let(:documents) do
          (1..3).map{ |i| { field: "test#{i}" }}
        end

        before do
          authorized_collection.insert_many(documents)
          allow(view.collection.client.cluster).to receive(:single?).and_return(false)
        end

        after do
          client.close
        end

        let(:client) do
          authorized_client.with(server_selection_timeout: 1)
        end

        let(:collection) do
          client[authorized_collection.name]
        end

        let(:view) do
          Mongo::Collection::View.new(collection, selector, options)
        end

        let(:distinct) do
          read_preference = { :mode => :secondary, :tag_sets => [{ 'non' => 'existent' }] }
          view.distinct(:field, read: read_preference)
        end

        it 'uses the read preference passed to the method' do
          expect {
            distinct
          }.to raise_exception(Mongo::Error::NoServerAvailable)
        end
      end

      context 'when a read preference is set on the view' do

        let(:view_with_read_pref) do
          view.read(:mode => :secondary, :tag_sets => [{ 'non' => 'existent' }])
        end

        let(:distinct) do
          view_with_read_pref.distinct(:field, read: { mode: :primary })
        end

        it 'uses the read preference passed to the method' do
          expect(distinct.sort).to eq([ 'test1', 'test2', 'test3' ])
        end
      end
    end

    context 'when a max_time_ms is specified', if: write_command_enabled? do

      let(:documents) do
        (1..3).map{ |i| { field: "test" }}
      end

      before do
        authorized_collection.insert_many(documents)
      end

      it 'sets the max_time_ms option on the command' do
        expect {
          view.distinct(:field, max_time_ms: 0.1)
        }.to raise_error(Mongo::Error::OperationFailure)
      end

      it 'sets the max_time_ms option on the command' do
        expect(view.distinct(:field, max_time_ms: 100)).to eq([ 'test' ])
      end
    end

    context 'when the field does not exist' do

      it 'returns an empty array' do
        expect(view.distinct(:nofieldexists)).to be_empty
      end
    end

    context 'when a collation is specified on the view' do

      let(:result) do
        view.distinct(:name)
      end

      before do
        authorized_collection.insert_one(name: 'bang')
        authorized_collection.insert_one(name: 'BANG')
      end

      let(:options) do
        { collation: { locale: 'en_US', strength: 2 } }
      end

      context 'when the server selected supports collations', if: collation_enabled? do

        it 'applies the collation to the distinct' do
          expect(result).to eq(['bang'])
        end
      end

      context 'when the server selected does not support collations', unless: collation_enabled? do

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

    context 'when a collation is specified in the method options' do

      let(:result) do
        view.distinct(:name, distinct_options)
      end

      before do
        authorized_collection.insert_one(name: 'bang')
        authorized_collection.insert_one(name: 'BANG')
      end

      let(:distinct_options) do
        { collation: { locale: 'en_US', strength: 2 } }
      end

      context 'when the server selected supports collations', if: collation_enabled? do

        it 'applies the collation to the distinct' do
          expect(result).to eq(['bang'])
        end
      end

      context 'when the server selected does not support collations', unless: collation_enabled? do

        it 'raises an exception' do
          expect {
            result
          }.to raise_exception(Mongo::Error::UnsupportedCollation)
        end

        context 'when a String key is used' do

          let(:distinct_options) do
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

    context 'when a collation is not specified' do

      let(:result) do
        view.distinct(:name)
      end

      before do
        authorized_collection.insert_one(name: 'bang')
        authorized_collection.insert_one(name: 'BANG')
      end

      it 'does not apply the collation to the distinct' do
        expect(result).to eq(['bang', 'BANG'])
      end
    end
  end

  describe '#hint' do

    context 'when a hint is specified' do

      let(:options) do
        { :hint => { 'x' => Mongo::Index::ASCENDING } }
      end

      let(:new_hint) do
        { 'x' => Mongo::Index::DESCENDING }
      end

      it 'sets the hint' do
        new_view = view.hint(new_hint)
        expect(new_view.hint).to eq(new_hint)
      end

      it 'returns a new View' do
        expect(view.hint(new_hint)).not_to be(view)
      end
    end

    context 'when a hint is not specified' do

      let(:options) do
        { :hint => 'x' }
      end

      it 'returns the hint' do
        expect(view.hint).to eq(options[:hint])
      end
    end
  end

  describe '#limit' do

    context 'when a limit is specified' do

      let(:options) do
        { :limit => 5 }
      end

      let(:new_limit) do
        10
      end

      it 'sets the limit' do
        new_view = view.limit(new_limit)
        expect(new_view.limit).to eq(new_limit)
      end

      it 'returns a new View' do
        expect(view.limit(new_limit)).not_to be(view)
      end
    end

    context 'when a limit is not specified' do

      let(:options) do
        { :limit => 5 }
      end

      it 'returns the limit' do
        expect(view.limit).to eq(options[:limit])
      end
    end
  end

  describe '#max_scan' do

    let(:new_view) do
      view.max_scan(10)
    end

    it 'sets the value in the options' do
      expect(new_view.max_scan).to eq(10)
    end
  end

  describe '#max_value' do

    let(:new_view) do
      view.max_value(_id: 1)
    end

    it 'sets the value in the options' do
      expect(new_view.max_value).to eq('_id' => 1)
    end
  end

  describe '#min_value' do

    let(:new_view) do
      view.min_value(_id: 1)
    end

    it 'sets the value in the options' do
      expect(new_view.min_value).to eq('_id' => 1)
    end
  end

  describe '#no_cursor_timeout' do

    let(:new_view) do
      view.no_cursor_timeout
    end

    it 'sets the flag' do
      expect(new_view.options[:no_cursor_timeout]).to be true
    end

    it 'returns a new View' do
      expect(new_view).not_to be(view)
    end
  end

  describe '#projection' do

    let(:options) do
      { :projection => { 'x' => 1 } }
    end

    context 'when projection are specified' do

      let(:new_projection) do
        { 'y' => 1 }
      end

      before do
        authorized_collection.insert_one(y: 'value', a: 'other_value')
      end

      it 'sets the projection' do
        new_view = view.projection(new_projection)
        expect(new_view.projection).to eq(new_projection)
      end

      it 'returns a new View' do
        expect(view.projection(new_projection)).not_to be(view)
      end

      it 'returns only that field on the collection' do
        expect(view.projection(new_projection).first.keys).to match_array(['_id', 'y'])
      end
    end

    context 'when projection is not specified' do

      it 'returns the projection' do
        expect(view.projection).to eq(options[:projection])
      end
    end

    context 'when projection is not a document' do

      let(:new_projection) do
        'y'
      end

      it 'raises an error' do
        expect do
          view.projection(new_projection)
        end.to raise_error(Mongo::Error::InvalidDocument)
      end
    end
  end

  describe '#read' do

    context 'when providing a hash' do

      it 'converts to a read preference' do
        expect(view.read(:mode => :primary_preferred).read).to be_a(
          Mongo::ServerSelector::PrimaryPreferred
        )
      end
    end

    context 'when a read pref is specified' do

      let(:options) do
        { :read => Mongo::ServerSelector.get(:mode => :secondary) }
      end

      let(:new_read) do
        Mongo::ServerSelector.get(:mode => :secondary_preferred)
      end

      it 'sets the read preference' do
        new_view = view.read(new_read)
        expect(new_view.read).to eq(new_read)
      end

      it 'returns a new View' do
        expect(view.read(new_read)).not_to be(view)
      end
    end

    context 'when a read pref is not specified' do

      let(:options) do
        { :read =>  Mongo::ServerSelector.get(:mode => :secondary) }
      end

      it 'returns the read preference' do
        expect(view.read).to eq(options[:read])
      end

      context 'when no read pref is set on initialization' do

        let(:options) do
          {}
        end

        it 'returns the collection read preference' do
          expect(view.read).to eq(authorized_collection.read_preference)
        end
      end
    end
  end

  describe '#show_disk_loc' do

    let(:options) do
      { :show_disk_loc => true }
    end

    context 'when show_disk_loc is specified' do

      let(:new_show_disk_loc) do
        false
      end

      it 'sets the show_disk_loc value' do
        new_view = view.show_disk_loc(new_show_disk_loc)
        expect(new_view.show_disk_loc).to eq(new_show_disk_loc)
      end

      it 'returns a new View' do
        expect(view.show_disk_loc(new_show_disk_loc)).not_to be(view)
      end
    end

    context 'when show_disk_loc is not specified' do

      it 'returns the show_disk_loc value' do
        expect(view.show_disk_loc).to eq(options[:show_disk_loc])
      end
    end
  end

  describe '#modifiers' do

    let(:options) do
      { :modifiers => { '$orderby' => 1 } }
    end

    context 'when a modifiers document is specified' do

      let(:new_modifiers) do
        { '$orderby' => -1 }
      end

      it 'sets the new_modifiers document' do
        new_view = view.modifiers(new_modifiers)
        expect(new_view.modifiers).to eq(new_modifiers)
      end

      it 'returns a new View' do
        expect(view.modifiers(new_modifiers)).not_to be(view)
      end
    end

    context 'when a modifiers document is not specified' do

      it 'returns the modifiers value' do
        expect(view.modifiers).to eq(options[:modifiers])
      end
    end
  end

  describe '#max_time_ms' do

    let(:options) do
      { :max_time_ms => 200 }
    end

    context 'when max_time_ms is specified' do

      let(:new_max_time_ms) do
        300
      end

      it 'sets the max_time_ms value' do
        new_view = view.max_time_ms(new_max_time_ms)
        expect(new_view.max_time_ms).to eq(new_max_time_ms)
      end

      it 'returns a new View' do
        expect(view.max_time_ms(new_max_time_ms)).not_to be(view)
      end
    end

    context 'when max_time_ms is not specified' do

      it 'returns the max_time_ms value' do
        expect(view.max_time_ms).to eq(options[:max_time_ms])
      end
    end
  end

  describe '#cusor_type' do

    let(:options) do
      { :cursor_type => :tailable }
    end

    context 'when cursor_type is specified' do

      let(:new_cursor_type) do
        :tailable_await
      end

      it 'sets the cursor_type value' do
        new_view = view.cursor_type(new_cursor_type)
        expect(new_view.cursor_type).to eq(new_cursor_type)
      end

      it 'returns a new View' do
        expect(view.cursor_type(new_cursor_type)).not_to be(view)
      end
    end

    context 'when cursor_type is not specified' do

      it 'returns the cursor_type value' do
        expect(view.cursor_type).to eq(options[:cursor_type])
      end
    end
  end

  describe '#skip' do

    context 'when a skip is specified' do

      let(:options) do
        { :skip => 5 }
      end

      let(:new_skip) do
        10
      end

      it 'sets the skip value' do
        new_view = view.skip(new_skip)
        expect(new_view.skip).to eq(new_skip)
      end

      it 'returns a new View' do
        expect(view.skip(new_skip)).not_to be(view)
      end
    end

    context 'when a skip is not specified' do

      let(:options) do
        { :skip => 5 }
      end

      it 'returns the skip value' do
        expect(view.skip).to eq(options[:skip])
      end
    end
  end

  describe '#snapshot' do

    let(:new_view) do
      view.snapshot(true)
    end

    it 'sets the value in the options' do
      expect(new_view.snapshot).to be true
    end
  end

  describe '#sort' do

    context 'when a sort is specified' do

      let(:options) do
        { :sort => { 'x' => Mongo::Index::ASCENDING }}
      end

      let(:new_sort) do
        { 'x' => Mongo::Index::DESCENDING }
      end

      it 'sets the sort option' do
        new_view = view.sort(new_sort)
        expect(new_view.sort).to eq(new_sort)
      end

      it 'returns a new View' do
        expect(view.sort(new_sort)).not_to be(view)
      end
    end

    context 'when a sort is not specified' do

      let(:options) do
        { :sort => { 'x' => Mongo::Index::ASCENDING }}
      end

      it 'returns the sort' do
        expect(view.sort).to eq(options[:sort])
      end
    end
  end
end
