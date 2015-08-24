require 'spec_helper'

describe Mongo::Collection::View do

  let(:selector) do
    {}
  end

  let(:options) do
    {}
  end

  let(:view) do
    described_class.new(authorized_collection, selector, options)
  end

  after do
    authorized_collection.delete_many
  end

  context 'when query modifiers are provided' do

    context 'when a selector has a query modifier' do

      let(:options) do
        {}
      end

      let(:expected_modifiers) do
        BSON::Document.new(selector)
      end

      let(:parsed_selector) do
        {}
      end

      let(:query_selector) do
        BSON::Document.new(selector)
      end

      context 'when the $query key is a string' do

        let(:selector) do
          { "$query" => { a: 1 }, :$someMod => 100 }
        end

        let(:expected_modifiers) do
          BSON::Document.new(selector)
        end

        it 'sets the modifiers' do
          expect(view.instance_variable_get(:@modifiers)).to eq(expected_modifiers)
        end

        it 'removes the modifiers from the selector' do
          expect(view.selector).to eq(parsed_selector)
        end

        it 'creates the correct query selector' do
          expect(view.send(:query_spec)[:selector]).to eq(query_selector)
        end

      end

      context 'when the $query key is a symbol' do

        let(:selector) do
          { :$query => { a: 1 }, :$someMod => 100 }
        end

        let(:expected_modifiers) do
          BSON::Document.new(selector)
        end

        it 'sets the modifiers' do
          expect(view.instance_variable_get(:@modifiers)).to eq(expected_modifiers)
        end

        it 'removes the modifiers from the selector' do
          expect(view.selector).to eq(parsed_selector)
        end

        it 'creates the correct query selector' do
          expect(view.send(:query_spec)[:selector]).to eq(query_selector)
        end
      end
    end

    context 'when a modifiers document is provided in the options' do

      let(:selector) do
        { a: 1 }
      end

      let(:options) do
        { :modifiers => { :$someMod => 100 } }
      end

      let(:expected_modifiers) do
        options[:modifiers]
      end

      let(:parsed_selector) do
        { a: 1 }
      end

      let(:query_selector) do
        BSON::Document.new(:$query => { a: 1 }, :$someMod => 100)
      end

      it 'sets the modifiers' do
        expect(view.instance_variable_get(:@modifiers)).to eq(expected_modifiers)
      end

      it 'removes the modifiers from the selector' do
        expect(view.selector).to eq(parsed_selector)
      end

      it 'creates the correct query selector' do
        expect(view.send(:query_spec)[:selector]).to eq(query_selector)
      end

      context 'when modifiers and options are both provided' do

        let(:selector) do
          { a: 1 }
        end

        let(:options) do
          { :sort =>  { a: Mongo::Index::ASCENDING }, :modifiers => { :$orderby => { a: Mongo::Index::DESCENDING } } }
        end

        let(:expected_modifiers) do
          { :$orderby => options[:sort] }
        end

        let(:parsed_selector) do
          { a: 1 }
        end

        let(:query_selector) do
          BSON::Document.new(:$query => selector, :$orderby => { a: Mongo::Index::ASCENDING })
        end

        it 'sets the modifiers' do
          expect(view.instance_variable_get(:@modifiers)).to eq(expected_modifiers)
        end

        it 'removes the modifiers from the selector' do
          expect(view.selector).to eq(parsed_selector)
        end

        it 'creates the correct query selector' do
          expect(view.send(:query_spec)[:selector]).to eq(query_selector)
        end
      end

      context 'when modifiers, options and a query modifier are provided' do

        let(:selector) do
          { b: 2, :$query => { a: 1 }, :$someMod => 100 }
        end

        let(:options) do
          { :sort =>  { a: Mongo::Index::ASCENDING }, :modifiers => { :$someMod => true, :$orderby => { a: Mongo::Index::DESCENDING } } }
        end

        let(:expected_modifiers) do
          { :$query => { a: 1 }, :$orderby => { a: Mongo::Index::ASCENDING }, :$someMod => 100 }
        end

        let(:parsed_selector) do
          { b: 2 }
        end

        let(:query_selector) do
          BSON::Document.new(:$query => { a: 1 }, :$someMod => 100, :$orderby => { a: Mongo::Index::ASCENDING })
        end

        it 'sets the modifiers' do
          expect(view.instance_variable_get(:@modifiers)).to eq(expected_modifiers)
        end

        it 'removes the modifiers from the selector' do
          expect(view.selector).to eq(parsed_selector)
        end

        it 'creates the correct query selector' do
          expect(view.send(:query_spec)[:selector]).to eq(query_selector)
        end
      end
    end
  end

  describe '#==' do

    context 'when the other object is not a collection view' do

      let(:other) { 'test' }

      it 'returns false' do
        expect(view).to_not eq(other)
      end
    end

    context 'when the views have the same collection, selector, and options' do

      let(:other) do
        described_class.new(authorized_collection, selector, options)
      end

      it 'returns true' do
        expect(view).to eq(other)
      end
    end

    context 'when two views have a different collection' do

      let(:other_collection) do
        authorized_client[:other]
      end

      let(:other) do
        described_class.new(other_collection, selector, options)
      end

      it 'returns false' do
        expect(view).not_to eq(other)
      end
    end

    context 'when two views have a different selector' do

      let(:other_selector) do
        { 'name' => 'Emily' }
      end

      let(:other) do
        described_class.new(authorized_collection, other_selector, options)
      end

      it 'returns false' do
        expect(view).not_to eq(other)
      end
    end

    context 'when two views have different options' do

      let(:other_options) do
        { 'limit' => 20 }
      end

      let(:other) do
        described_class.new(authorized_collection, selector, other_options)
      end

      it 'returns false' do
        expect(view).not_to eq(other)
      end
    end
  end

  describe 'copy' do

    let(:view_clone) do
      view.clone
    end

    it 'dups the options' do
      expect(view.options).not_to be(view_clone.options)
    end

    it 'dups the selector' do
      expect(view.selector).not_to be(view_clone.selector)
    end

    it 'references the same collection' do
      expect(view.collection).to be(view_clone.collection)
    end
  end

  describe '#each' do

    let(:documents) do
      (1..10).map{ |i| { field: "test#{i}" }}
    end

    before do
      authorized_collection.insert_many(documents)
    end

    after do
      authorized_collection.delete_many
    end

    context 'when sending the initial query' do

      let(:returned) do
        view.to_a
      end

      let(:query_spec) do
        view.send(:query_spec)
      end

      context 'when limit is specified' do

        let(:options) do
          { :limit => 5 }
        end

        let(:returned) do
          view.to_a
        end

        it 'sets the limit on the initial query' do
          expect(query_spec[:options][:limit]).to eq(options[:limit])
        end

        it 'returns limited documents' do
          expect(returned.count).to eq(5)
        end

        it 'iterates over all of the documents' do
          returned.each do |doc|
            expect(doc).to have_key('field')
          end
        end
      end

      context 'when batch size is specified' do

        let(:options) do
          { :batch_size => 5 }
        end

        let(:returned) do
          view.to_a
        end

        it 'sets the batch size on the initial query' do
          expect(query_spec[:options][:batch_size]).to eq(options[:batch_size])
        end

        it 'returns all the documents' do
          expect(returned.count).to eq(10)
        end

        it 'iterates over all of the documents' do
          returned.each do |doc|
            expect(doc).to have_key('field')
          end
        end
      end

      context 'when no limit is specified' do

        it 'does not set a limit on the initial query' do
          expect(query_spec[:options][:limit]).to be_nil
        end

        it 'returns all the documents' do
          expect(returned.count).to eq(10)
        end

        it 'iterates over all of the documents' do
          returned.each do |doc|
            expect(doc).to have_key('field')
          end
        end
      end

      context 'when batch size is greater than limit' do

        let(:options) do
          { :batch_size => 5, :limit => 3 }
        end

        let(:returned) do
          view.to_a
        end

        it 'sets the limit on the initial query' do
          expect(query_spec[:options][:limit]).to eq(options[:limit])
        end

        it 'returns the limit of documents' do
          expect(returned.count).to eq(3)
        end

        it 'iterates over all of the documents' do
          returned.each do |doc|
            expect(doc).to have_key('field')
          end
        end
      end

      context 'when limit is greater than batch size' do

        let(:options) do
          { :limit => 5, :batch_size => 3 }
        end

        let(:returned) do
          view.to_a
        end

        it 'sets the batch size on the initial query' do
          expect(query_spec[:options][:batch_size]).to eq(options[:batch_size])
        end

        it 'sets the limit on the initial query' do
          expect(query_spec[:options][:limit]).to eq(options[:limit])
        end

        it 'returns the limit of documents' do
          expect(returned.count).to eq(5)
        end

        it 'iterates over all of the documents' do
          returned.each do |doc|
            expect(doc).to have_key('field')
          end
        end
      end

      context 'when the selector has special fields' do

        context 'when a snapshot option is specified' do

          let(:options) do
            { :snapshot => true }
          end

          before do
            expect(view).to receive(:special_selector).and_call_original
          end

          it 'creates a special query selector' do
            expect(query_spec[:selector][:$snapshot]).to eq(options[:snapshot])
          end

          it 'iterates over all of the documents' do
            returned.each do |doc|
              expect(doc).to have_key('field')
            end
          end
        end

        context 'when a max_scan option is provided' do

          let(:options) do
            { :max_scan => 100 }
          end

          before do
            expect(view).to receive(:special_selector).and_call_original
          end

          it 'creates a special query selector' do
            expect(query_spec[:selector][:$maxScan]).to eq(options[:max_scan])
          end

          it 'iterates over all of the documents' do
            returned.each do |doc|
              expect(doc).to have_key('field')
            end
          end
        end

        context 'when a max_time_ms option is provided' do

          let(:options) do
            { :max_time_ms => 100 }
          end

          before do
            expect(view).to receive(:special_selector).and_call_original
          end

          it 'creates a special query selector' do
            expect(query_spec[:selector][:$maxTimeMS]).to eq(options[:max_time_ms])
          end

          it 'iterates over all of the documents' do
            returned.each do |doc|
              expect(doc).to have_key('field')
            end
          end
        end

        context 'when a show_disk_loc option is provided' do

          let(:options) do
            { :show_disk_loc => true }
          end

          before do
            expect(view).to receive(:special_selector).and_call_original
          end

          it 'creates a special query selector' do
            expect(query_spec[:selector][:$showDiskLoc]).to eq(options[:show_disk_loc])
          end

          it 'iterates over all of the documents' do
            returned.each do |doc|
              expect(doc).to have_key('field')
            end
          end
        end
      end

      context 'when sorting' do

        let(:options) do
          { :sort => {'x' => Mongo::Index::ASCENDING }}
        end

        before do
          expect(view).to receive(:special_selector).and_call_original
        end

        it 'creates a special query selector' do
          expect(query_spec[:selector][:$orderby]).to eq(options[:sort])
        end

        it 'iterates over all of the documents' do
          returned.each do |doc|
            expect(doc).to have_key('field')
          end
        end
      end

      context 'when providing a hint' do

        context 'when the hint is bad' do

          let(:options) do
            { :hint => { 'x' => Mongo::Index::ASCENDING }}
          end

          before do
            expect(view).to receive(:special_selector).and_call_original
          end

          it 'creates a special query selector' do
            expect(query_spec[:selector][:$hint]).to eq(options[:hint])
          end
        end
      end

      context 'when providing a comment' do

        let(:options) do
          { :comment => 'query1' }
        end

        before do
          expect(view).to receive(:special_selector).and_call_original
        end

        it 'creates a special query selector' do
          expect(query_spec[:selector][:$comment]).to eq(options[:comment])
        end

        it 'iterates over all of the documents' do
          returned.each do |doc|
            expect(doc).to have_key('field')
          end
        end
      end

      context 'when the cluster is sharded', if: sharded? do

        before do
          expect(view).to receive(:special_selector).and_call_original
        end

        it 'iterates over all of the documents' do
          view.each do |doc|
            expect(doc).to have_key('field')
          end
        end

        context 'when there is a read preference' do

          let(:collection) do
            authorized_collection.with(read: { mode: :secondary})
          end

          let(:view) do
            described_class.new(collection, selector, options)
          end

          let(:formatted_read_pref) do
            BSON::Document.new(Mongo::ServerSelector.get(mode: :secondary).to_mongos)
          end

          it 'adds the formatted read preference to the selector' do
            expect(view.send(:query_spec)[:selector][:$readPreference]).to eq(formatted_read_pref)
          end
        end

        context 'when the read preference is primary' do

          let(:collection) do
            authorized_collection.with(read: { mode: :primary})
          end

          let(:view) do
            described_class.new(collection, selector, options)
          end

          it 'does not add the formatted read preference to the selector' do
            expect(view.send(:query_spec)[:selector][:$readPreference]).to be(nil)
          end
        end
      end

      context 'when a modifier document is provided' do

        let(:options) do
          { :modifiers => {
                            :$orderby => {'x' => Mongo::Index::ASCENDING }
                          }
          }
        end

        before do
          expect(view).to receive(:special_selector).and_call_original
        end

        it 'creates a special query selector' do
          expect(query_spec[:selector][:$orderby]).to eq(options[:modifiers][:$orderby])
        end

        it 'iterates over all of the documents' do
          view.each do |doc|
            expect(doc).to have_key('field')
          end
        end

        context 'when $explain is specified' do
          let(:options) do
            { :modifiers => {
                             :$explain => 1
                            }
            }
          end

          let(:explain) do
            view.to_a.first
          end

          it 'executes an explain' do
            expect(explain['cursor'] == 'BasicCursor' ||
                       explain['queryPlanner']).to be_truthy
          end

        end

        context 'when an option is also provided' do

          context 'when $orderby and sort are specified' do

            let(:options) do
              { :modifiers => {
                               :$orderby => { 'x' => Mongo::Index::ASCENDING }
                              },
                :sort => { 'x' => Mongo::Index::DESCENDING }
              }
            end

            it 'overrides the modifier value with the option value' do
              expect(query_spec[:selector][:$orderby]).to eq(options[:sort])
            end
          end

          context 'when $comment and comment are specified' do

            let(:options) do
              { :modifiers => {
                               :$comment => 'query1'
                              },
                :comment => 'query2'
              }
            end

            it 'overrides the modifier value with the option value' do
              expect(query_spec[:selector][:$comment]).to eq(options[:comment])
            end
          end

          context 'when $hint and hint are specified' do

            let(:options) do
              { :modifiers => {
                               :$hint => 'x'
                              },
                :hint => 'y'
              }
            end

            it 'overrides the modifier value with the option value' do
              expect(query_spec[:selector][:$hint]).to eq(options[:hint])
            end

          end

          context 'when $maxScan and max_scan are specified' do

            let(:options) do
              { :modifiers => {
                               :$maxScan => 4
                              },
                :max_scan => 5
              }
            end

            it 'overrides the modifier value with the option value' do
              expect(query_spec[:selector][:$maxScan]).to eq(options[:max_scan])
            end
          end

          context 'when $maxTimeMS and max_time_ms are specified' do

            let(:options) do
              { :modifiers => {
                               :$maxTimeMS => 100
                              },
                :max_time_ms => 200
              }
            end

            it 'overrides the modifier value with the option value' do
              expect(query_spec[:selector][:$maxTimeMS]).to eq(options[:max_time_ms])
            end
          end

          context 'when $query and a selector are specified' do

            let(:selector) do
              { 'y' => 1 }
            end

            let(:options) do
              { :modifiers => {
                                :$query => { 'field' => 1 }
                              }
              }
            end

            it 'overrides the modifier value with the option value' do
              expect(query_spec[:selector][:$query]).to eq(options[:modifiers][:$query])
            end
          end
        end
      end
    end

    context 'when there are no special fields' do

      before do
        expect(Mongo::Operation::Read::Query).to receive(:new) do |spec|
          expect(spec[:selector]).to eq(selector)
        end.and_call_original
      end

      it 'creates a normal query spec' do
        view.each do |doc|
          expect(doc).to have_key('field')
        end
      end
    end

    context 'when a block is not provided' do

      let(:enumerator) do
        view.each
      end

      it 'returns an enumerator' do
        enumerator.each do |doc|
          expect(doc).to have_key('field')
        end
      end
    end

    describe '#close_query' do

      let(:options) do
        { :batch_size => 1 }
      end

      before do
        e = view.to_enum
        e.next
        cursor = view.instance_variable_get(:@cursor)
        expect(cursor).to receive(:kill_cursors).and_call_original
      end

      it 'sends a kill cursors command for the cursor' do
        view.close_query
      end
    end
  end

  describe '#hash' do

    let(:other) do
      described_class.new(authorized_collection, selector, options)
    end

    it 'returns a unique value based on collection, selector, options' do
      expect(view.hash).to eq(other.hash)
    end

    context 'when two views only have different collections' do

      let(:other_collection) do
        authorized_client[:other]
      end

      let(:other) do
        described_class.new(other_collection, selector, options)
      end

      it 'returns different hash values' do
        expect(view.hash).not_to eq(other.hash)
      end
    end

    context 'when two views only have different selectors' do

      let(:other_selector) do
        { 'name' => 'Emily' }
      end

      let(:other) do
        described_class.new(authorized_collection, other_selector, options)
      end

      it 'returns different hash values' do
        expect(view.hash).not_to eq(other.hash)
      end
    end

    context 'when two views only have different options' do

      let(:other_options) do
        { 'limit' => 20 }
      end

      let(:other) do
        described_class.new(authorized_collection, selector, other_options)
      end

      it 'returns different hash values' do
        expect(view.hash).not_to eq(other.hash)
      end
    end
  end

  describe '#initialize' do

    let(:options) do
      { :limit => 5 }
    end

    it 'sets the collection' do
      expect(view.collection).to eq(authorized_collection)
    end

    it 'sets the selector' do
      expect(view.selector).to eq(selector)
    end

    it 'dups the selector' do
      expect(view.selector).not_to be(selector)
    end

    it 'sets the options' do
      expect(view.options).to eq(options)
    end

    it 'dups the options' do
      expect(view.options).not_to be(options)
    end

    context 'when the selector is not a valid document' do

      let(:selector) do
        'y'
      end

      it 'raises an error' do
        expect do
          view
        end.to raise_error(Mongo::Error::InvalidDocument)
      end
    end
  end

  describe '#inspect' do

    context 'when there is a namespace, selector, and options' do

      let(:options) do
        { :limit => 5 }
      end

      let(:selector) do
        { 'name' => 'Emily' }
      end

      it 'returns a string' do
        expect(view.inspect).to be_a(String)
      end

      it 'returns a string containing the collection namespace' do
        expect(view.inspect).to match(/.*#{authorized_collection.namespace}.*/)
      end

      it 'returns a string containing the selector' do
        expect(view.inspect).to match(/.*#{selector.inspect}.*/)
      end

      it 'returns a string containing the options' do
        expect(view.inspect).to match(/.*#{options.inspect}.*/)
      end
    end
  end
end
