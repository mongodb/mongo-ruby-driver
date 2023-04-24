# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Mongos pinning' do
  require_topology :sharded
  min_server_fcv '4.2'

  let(:client) { authorized_client }
  let(:collection) { client['mongos_pinning_spec'] }

  before do
    collection.create
  end

  context 'successful operations' do
    it 'pins and unpins' do
      session = client.start_session
      expect(session.pinned_server).to be nil

      session.start_transaction
      expect(session.pinned_server).to be nil

      primary = client.cluster.next_primary

      collection.insert_one({a: 1}, session: session)
      expect(session.pinned_server).not_to be nil

      session.commit_transaction
      expect(session.pinned_server).not_to be nil

      collection.insert_one({a: 1}, session: session)
      expect(session.pinned_server).to be nil
    end
  end
end
