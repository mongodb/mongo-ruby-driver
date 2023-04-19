# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Server description' do
  clean_slate

  let(:client) { ClientRegistry.instance.global_client('authorized') }
  let(:desc) do
    client.cluster.next_primary.description
  end

  let!(:start_time) { Time.now }

  describe '#op_time' do
    require_topology :replica_set
    min_server_fcv '3.4'

    it 'is set' do
      expect(desc).not_to be_unknown

      expect(desc.op_time).to be_a(BSON::Timestamp)
    end
  end

  describe '#last_write_date' do
    require_topology :replica_set
    min_server_fcv '3.4'

    it 'is set' do
      expect(desc).not_to be_unknown

      expect(desc.last_write_date).to be_a(Time)
    end
  end

  describe '#last_update_time' do

    it 'is set' do
      expect(desc).not_to be_unknown

      expect(desc.last_update_time).to be_a(Time)
      # checked while this test was running
      expect(desc.last_update_time).to be > start_time
    end
  end
end
