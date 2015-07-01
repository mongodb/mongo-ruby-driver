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
    authorized_collection.find.delete_many
  end

  describe '#allow_partial_results' do

    let(:new_view) do
      view.allow_partial_results
    end

    it 'sets the flag' do
      expect(new_view.send(:flags)).to include(:partial)
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
      authorized_collection.find.delete_many
    end

    context 'when a selector is provided' do

      let(:selector) do
        { field: 'test1' }
      end

      it 'returns the count of matching documents' do
        expect(view.count).to eq(1)
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
  end

  describe '#distinct' do

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
          expect(distinct).to eq([ 'test1', 'test2', 'test3' ])
        end
      end

      context 'when the field is a string' do

        let(:distinct) do
          view.distinct('field')
        end

        it 'returns the distinct values' do
          expect(distinct).to eq([ 'test1', 'test2', 'test3' ])
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

    context 'when a read preference is specified' do

      let(:documents) do
        (1..3).map{ |i| { field: "test#{i}" }}
      end

      before do
        authorized_collection.insert_many(documents)
      end

      let(:distinct) do
        view.distinct(:field, read: { mode: :secondary })
      end

      it 'returns the distinct values' do
        expect(distinct).to eq([ 'test1', 'test2', 'test3' ])
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

  describe '#no_cursor_timeout' do

    let(:new_view) do
      view.no_cursor_timeout
    end

    it 'sets the flag' do
      expect(new_view.send(:flags)).to include(:no_cursor_timeout)
    end

    it 'returns a new View' do
      expect(new_view).not_to be(view)
    end
  end

  describe '#parallel_scan', unless: sharded? do

    let(:documents) do
      (1..200).map do |i|
        { name: "testing-scan-#{i}" }
      end
    end

    before do
      authorized_collection.insert_many(documents)
    end

    let(:cursors) do
      view.parallel_scan(2)
    end

    it 'returns an array of cursors', if: write_command_enabled? do
      cursors.each do |cursor|
        expect(cursor.class).to be(Mongo::Cursor)
      end
    end

    it 'returns the correct number of documents', if: write_command_enabled? do
      expect(
        cursors.reduce(0) { |total, cursor| total + cursor.to_a.size }
      ).to eq(200)
    end

    it 'raises an error', unless: write_command_enabled? do
      expect {
        cursors
      }.to raise_error(Mongo::Error::OperationFailure)
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

      it 'sets the projection' do
        new_view = view.projection(new_projection)
        expect(new_view.projection).to eq(new_projection)
      end

      it 'returns a new View' do
        expect(view.projection(new_projection)).not_to be(view)
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
      { :modifiers => { :$orderby => Mongo::Index::ASCENDING } }
    end

    context 'when a modifiers document is specified' do

      let(:new_modifiers) do
        { :modifiers => { :$orderby => Mongo::Index::DESCENDING } }
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

    context 'when an option is a cursor flag' do

      let(:query_spec_options) do
        view.send(:query_spec)[:options]
      end

      context 'when allow_partial_results is set as an option' do

        let(:options) do
          { :allow_partial_results => true }
        end

        it 'sets the cursor flag' do
          expect(query_spec_options[:flags]).to eq([:partial])
        end

        context 'when allow_partial_results is also called as a method' do

          before do
            view.allow_partial_results
          end

          it 'sets only one cursor flag' do
            expect(query_spec_options[:flags]).to eq([:partial])
          end
        end
      end

      context 'when oplog_replay is set as an option' do

        let(:options) do
          { :oplog_replay => true }
        end

        it 'sets the cursor flag' do
          expect(query_spec_options[:flags]).to eq([:oplog_replay])
        end
      end

      context 'when oplog_replay is set as an option' do

        let(:options) do
          { :no_cursor_timeout => true }
        end

        it 'sets the cursor flag' do
          expect(query_spec_options[:flags]).to eq([:no_cursor_timeout])
        end
      end

      context 'when cursor_type is set as an option' do

        context 'when :tailable is the cursor type' do

          let(:options) do
            { :cursor_type => :tailable }
          end

          it 'sets the cursor flag' do
            expect(query_spec_options[:flags]).to eq([:tailable_cursor])
          end
        end

        context 'when :tailable_await is the cursor type' do

          let(:options) do
            { :cursor_type => :tailable_await }
          end

          it 'sets the cursor flag' do
            expect(query_spec_options[:flags]).to eq([:await_data, :tailable_cursor])
          end
        end
      end
    end
  end
end
