require 'spec_helper'

describe Mongo::Scope do

  let(:db) { double("db") }
  let(:collection) { double("collection") }

  let(:selector) { {} }
  let(:opts) { {} }

  let(:ascending) { 1 }
  let(:descending) { -1 }

  let(:scope) do
    db.stub(:name) { TEST_DB }
    collection.stub(:name) { TEST_COLL }
    collection.stub(:db) { db }
    described_class.new(collection, selector, opts)
  end

  describe '#initialize' do
    let(:opts) { {:limit => 5} }

    it 'sets the collection' do
      expect(scope.collection).to eq(collection)
    end

    it 'sets the selector' do
      expect(scope.selector).to eq(selector)
    end

    it 'sets the options' do
      expect(scope.limit).to eq(5)
    end

  end

  describe '#inspect' do
    it 'returns a string' do
      expect(scope.inspect).to be_String
    end

  end

  describe '#comment' do

    context 'when a comment is specified' do
      let(:opts) { { :comment => "test1" } }
      let(:new_comment) { "test2" }

      it 'sets the comment' do
        scope.comment(new_comment)
        expect(scope.comment).to eq(new_comment)
      end

      it 'returns self' do
        scope_self = scope
        expect(scope.comment(new_comment)).to eq(scope_self)
      end
    end

    context 'when a comment is not specified' do
      let(:opts) { { :comment => "test1" } }

      it 'returns the comment' do
        expect(scope.comment).to eq(opts[:comment])
      end
    end

  end

  describe '#fields' do

    context 'when fields are specified' do
      let(:opts) { {:fields => {"x" => 1 } } }
      let(:new_fields) { { "y" => 1 } }

      it 'sets the fields' do
        scope.fields(new_fields)
        expect(scope.fields).to eq(new_fields)
      end

      it 'returns self' do
        scope_self = scope
        expect(scope.fields(new_fields)).to eq(scope_self)
      end
    end

    context 'when fields are not specified' do
      let(:opts) { {:fields => {"x" => 1 } } }

      it 'returns the fields' do
        expect(scope.fields).to eq(opts[:fields])
      end
    end

  end

  describe '#hint' do

    context 'when a hint is specified' do
      let(:opts) { {:hint => {"x" => ascending} } }
      let(:new_hint) { {"x" => descending} }

      it 'sets the hint' do
        scope.hint(new_hint)
        expect(scope.hint).to eq(new_hint)
      end

      it 'returns self' do
        scope_self = scope
        expect(scope.hint(new_hint)).to eq(scope_self)
      end
    end

    context 'when a hint is not specified' do
      let(:opts) { {:hint => "x" } }

      it 'returns the hint' do
        expect(scope.hint).to eq(opts[:hint])
      end
    end

  end

  describe '#limit' do

    context 'when a limit is specified' do
      let(:opts) { {:limit => 5} }
      let(:new_limit) { 10 }

      it 'sets the limit' do
        scope.limit(new_limit)
        expect(scope.limit).to eq(new_limit)
      end

      it 'returns self' do
        scope_self = scope
        expect(scope.limit(new_limit)).to eq(scope_self)
      end
    end

    context 'when a limit is not specified' do
      let(:opts) { {:limit => 5 } }

      it 'returns the limit' do
        expect(scope.limit).to eq(opts[:limit])
      end
    end

  end

  describe '#skip' do

    context 'when a skip is specified' do
      let(:opts) { {:skip => 5} }
      let(:new_skip) { 10 }

      it 'sets the skip value' do
        scope.skip(new_skip)
        expect(scope.skip).to eq(new_skip)
      end

      it 'returns self' do
        scope_self = scope
        expect(scope.skip(new_skip)).to eq(scope_self)
      end
    end

    context 'when a skip is not specified' do
      let(:opts) { {:skip => 5 } }

      it 'returns the skip value' do
        expect(scope.skip).to eq(opts[:skip])
      end
    end

  end

  describe '#max_scan' do

    context 'when a max_scan is specified' do
      let(:opts) { {:max_scan => 5} }
      let(:new_max_scan) { 10 }

      it 'sets the max_scan' do
        scope.max_scan(new_max_scan)
        expect(scope.max_scan).to eq(new_max_scan)
      end

      it 'returns self' do
        scope_self = scope
        expect(scope.max_scan(new_max_scan)).to eq(scope_self)
      end
    end

    context 'when a max_scan is not specified' do
      let(:opts) { {:max_scan => 5 } }

      it 'returns the max_scan' do
        expect(scope.max_scan).to eq(opts[:max_scan])
      end
    end

  end

  describe '#read' do

    context 'when a read option is specified' do
      let(:opts) { {:read =>  :secondary} }
      let(:new_read) { :secondary_preferred }

      it 'sets the read preference' do
        scope.read(new_read)
        expect(scope.read).to eq(new_read)
      end

      it 'returns self' do
        scope_self = scope
        expect(scope.read(new_read)).to eq(scope_self)
      end
    end

    context 'when a read is not specified' do
      let(:opts) { {:read => :secondary } }

      it 'returns the read preference' do
        expect(scope.read).to eq(opts[:read])
      end

      context 'when no read is set on initializaiton' do
        let(:opts) { {} }
        let(:collection_read) {:primary_preferred}

        it 'returns the collection read preference' do
          collection.stub(:read) { collection_read }
          expect(scope.read).to eq(collection_read)
        end

      end

    end

  end

  describe '#return_key' do

    context 'when a return_key option is specified' do
      let(:opts) { {:return_key => true} }
      let(:new_return_key) { false }

      it 'sets the return_key option' do
        scope.return_key(new_return_key)
        expect(scope.return_key).to eq(new_return_key)
      end

      it 'returns self' do
        scope_self = scope
        expect(scope.return_key(new_return_key)).to eq(scope_self)
      end
    end

    context 'when a return_key option is not specified' do
      let(:opts) { { :return_key => true } }

      it 'returns the return_key' do
        expect(scope.return_key).to eq(opts[:return_key])
      end
    end

  end

  describe '#show_disk_loc' do

    context 'when a show_disk_loc option is specified' do
      let(:opts) { {:show_disk_loc => true} }
      let(:new_show_disk_loc) { false }

      it 'sets the show_disk_loc option' do
        scope.show_disk_loc(new_show_disk_loc)
        expect(scope.show_disk_loc).to eq(new_show_disk_loc)
      end

      it 'returns self' do
        scope_self = scope
        expect(scope.show_disk_loc(new_show_disk_loc)).to eq(scope_self)
      end
    end

    context 'when a show_disk_loc option is not specified' do
      let(:opts) { { :show_disk_loc => true } }

      it 'returns the show_disk_loc' do
        expect(scope.show_disk_loc).to eq(opts[:show_disk_loc])
      end
    end

  end

  describe '#snapshot' do

    context 'when a snapshot option is specified' do
      let(:opts) { {:snapshot => true} }
      let(:new_snapshot) { false }

      it 'sets the snapshot option' do
        scope.snapshot(new_snapshot)
        expect(scope.snapshot).to eq(new_snapshot)
      end

      it 'returns self' do
        scope_self = scope
        expect(scope.snapshot(new_snapshot)).to eq(scope_self)
      end
    end

    context 'when a snapshot option is not specified' do
      let(:opts) { { :snapshot => true } }

      it 'returns the snapshot' do
        expect(scope.snapshot).to eq(opts[:snapshot])
      end
    end

  end

  describe '#sort' do

    context 'when a sort is specified' do
      let(:opts) { { "x" => ascending } }
      let(:new_sort) { {"x" => descending }  }

      it 'sets the sort option' do
        scope.sort(new_sort)
        expect(scope.sort).to eq(new_sort)
      end

      it 'returns self' do
        scope_self = scope
        expect(scope.sort(new_sort)).to eq(scope_self)
      end
    end

    context 'when a sort is not specified' do
      let(:opts) { { "x" => ascending } }

      it 'returns the sort' do
        expect(scope.sort).to eq(opts[:sort])
      end
    end

  end

  describe '#count' do
    let(:client) { double("client") }

    it "calls count on collection" do
      collection.stub(:client) { client }
      collection.stub(:count) { 10 }
      expect(scope.count).to eq(10)
    end

  end

  describe '#explain' do
    let(:client) { double("client") }

    it "calls explain on collection" do
      collection.stub(:client) { client }
      collection.stub(:explain) { {"n" => 10, "nscanned" => 11} }
      expect(scope.explain).to eq({"n" => 10, "nscanned" => 11})
    end

  end

  describe '#intersect' do

    context 'when there is already a selector and opts defined' do
      let(:selector) { {"x" => 1} }
      let(:opts) { {:limit => 5, :comment => "emily"} }

      it 'merges the new selector with the existing one' do
        scope.intersect({"x" => 2, "y" => 1})
        expect(scope.selector).to eq({"x" => 2, "y" => 1})
      end

      it 'doesnt alter the query opts' do
        scope.intersect({"x" => 2, "y" => 1})
        expect(scope.limit).to eq(5)
        expect(scope.comment).to eq("emily")
      end

    end

    context 'when there is no selector defined' do
      let(:opts) { {:limit => 5, :comment => "emily"} }

      it 'merges the new selector with the existing one' do
        scope.intersect({"x" => 2, "y" => 1})
        expect(scope.selector).to eq({"x" => 2, "y" => 1})
      end

      it 'doesnt alter the query opts' do
        scope.intersect({"x" => 2, "y" => 1})
        expect(scope.limit).to eq(5)
        expect(scope.comment).to eq("emily")
      end

    end

  end

  describe 'chaining' do

    context 'when an intersect is chained with a helper method' do

      it 'alters the scope' do
        scope.intersect(selector).limit(5).skip(10)
        expect(scope.selector).to eq(selector)
        expect(scope.limit).to eq(5)
        expect(scope.skip).to eq(10)
      end

    end

    context 'when a scope is chained with a terminator' do
      let(:scope ) { described_class.new(collection, selector, opts) }
      let(:client) { double("client") }

      it 'terminates the chaining' do
        collection.stub(:client) { client }
        collection.stub(:count) { 10 }
        expect(scope.limit(5).skip(10).count).to eq(10)
      end

    end

  end

end
