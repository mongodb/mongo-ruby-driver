require 'spec_helper'

describe 'CRUD operations' do
  let(:collection) { authorized_client['crud_integration'] }

  before do
    collection.delete_many
  end

  describe 'upsert' do
    context 'with default write concern' do
      it 'upserts' do
        collection.count_documents({}).should == 0

        res = collection.find(_id: 'foo').update_one({'$set' => {foo: 'bar'}}, upsert: true)
        p res

        res.documents.first['upserted'].length.should == 1

        collection.count_documents({}).should == 1
      end
    end

    context 'unacknowledged write' do
      let(:unack_collection) do
        collection.with(write_concern: {w: 0})
      end

      before do
        unack_collection.write_concern.acknowledged?.should be false
      end

      it 'upserts' do
        unack_collection.count_documents({}).should == 0

        res = unack_collection.find(_id: 'foo').update_one({'$set' => {foo: 'bar'}}, upsert: true)

        # since write concern is unacknowledged, wait for the data to be
        # persisted (hopefully)
        sleep 0.25

        unack_collection.count_documents({}).should == 1
      end
    end
  end
end
