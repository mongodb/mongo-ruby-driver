# frozen_string_literal: true

require 'spec_helper'

describe 'Mongos pinning' do
  require_topology :sharded

  let(:client) { authorized_client }
  let(:collection) { client['mongos_pinning_spec'] }

  before do
    collection.create
  end

  context 'successful operations' do
    it 'pins and unpins' do
      session = client.start_session
      expect(session.pinned_server).to be_nil

      session.start_transaction
      expect(session.pinned_server).to be_nil

      client.cluster.next_primary

      collection.insert_one({ a: 1 }, session: session)
      expect(session.pinned_server).not_to be_nil

      session.commit_transaction
      expect(session.pinned_server).not_to be_nil

      collection.insert_one({ a: 1 }, session: session)
      expect(session.pinned_server).to be_nil
    end
  end
end
