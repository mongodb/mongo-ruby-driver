require 'spec_helper'

describe 'Change stream integration' do
  before do
    unless test_change_streams?
      skip 'Not testing change streams'
    end
  end

  describe 'next' do
    it 'returns changes' do
      cs = authorized_collection.watch

      authorized_collection.insert_one(:a => 1)

      change = cs.to_enum.next
      expect(change).to be_a(BSON::Document)
      expect(change['operationType']).to eql('insert')
      doc = change['fullDocument']
      expect(doc['_id']).to be_a(BSON::ObjectId)
      doc.delete('_id')
      expect(doc).to eql('a' => 1)
    end
  end

  describe 'try_next' do
    context 'there are changes' do
      it 'returns changes' do
        cs = authorized_collection.watch

        authorized_collection.insert_one(:a => 1)

        change = cs.to_enum.try_next
        expect(change).to be_a(BSON::Document)
        expect(change['operationType']).to eql('insert')
        doc = change['fullDocument']
        expect(doc['_id']).to be_a(BSON::ObjectId)
        doc.delete('_id')
        expect(doc).to eql('a' => 1)
      end
    end

    context 'there are no changes' do
      it 'returns nil' do
        cs = authorized_collection.watch

        change = cs.to_enum.try_next
        expect(change).to be nil
      end
    end
  end
end
