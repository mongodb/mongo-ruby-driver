# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Time zone querying' do
  let(:collection) { authorized_client[:time_zone_querying] }

  before do
    collection.delete_many
    collection.insert_many([
      {id: 1, created_at: Time.utc(2020, 10, 1, 23)},
      {id: 2, created_at: Time.utc(2020, 10, 2, 0)},
      {id: 3, created_at: Time.utc(2020, 10, 2, 1)},
    ])
  end

  context 'UTC time' do
    let(:time) { Time.utc(2020, 10, 1, 23, 22) }

    it 'finds correctly' do
      view = collection.find({created_at: {'$gt' => time}})
      expect(view.count).to eq(2)
      expect(view.map { |doc| doc[:id] }.sort).to eq([2, 3])
    end
  end

  context 'local time with zone' do
    let(:time) { Time.parse('2020-10-01T19:30:00-0500') }

    it 'finds correctly' do
      view = collection.find({created_at: {'$gt' => time}})
      expect(view.count).to eq(1)
      expect(view.first[:id]).to eq(3)
    end
  end

  context 'when ActiveSupport support is enabled' do
    before do
      unless SpecConfig.instance.active_support?
        skip "ActiveSupport support is not enabled"
      end
    end

    context 'ActiveSupport::TimeWithZone' do
      let(:time) { Time.parse('2020-10-01T19:30:00-0500').in_time_zone('America/New_York') }

      it 'finds correctly' do
        view = collection.find({created_at: {'$gt' => time}})
        expect(view.count).to eq(1)
        expect(view.first[:id]).to eq(3)
      end
    end
  end
end
