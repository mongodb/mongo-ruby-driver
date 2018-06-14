require 'spec_helper'

describe Mongo::Collection::View::ChangeStream do
  before do
    unless test_change_streams?
      skip 'Not testing change streams'
    end
  end

  it 'returns data' do
    cs = authorized_collection.watch

    authorized_collection.insert_one(:a => 1)

    change = cs.to_enum.next
    expect(change['operationType']).to eql('insert')
    doc = change['fullDocument']
    expect(doc['_id']).to be_a(BSON::ObjectId)
    doc.delete('_id')
    expect(doc).to eql('a' => 1)
  end
end
