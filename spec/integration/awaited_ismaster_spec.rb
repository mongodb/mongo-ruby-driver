# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'awaited hello' do
  min_server_fcv '4.4'

  # If we send the consecutive hello commands to different mongoses,
  # they have different process ids, and so the awaited one would return
  # immediately.
  require_no_multi_mongos

  let(:client) { authorized_client }

  it 'waits' do
    # Perform a regular hello to get topology version
    resp = client.database.command(hello: 1)
    doc = resp.replies.first.documents.first
    tv = Mongo::TopologyVersion.new(doc['topologyVersion'])
    tv.should be_a(BSON::Document)

    elapsed_time = Benchmark.realtime do
      resp = client.database.command(hello: 1,
        topologyVersion: tv.to_doc, maxAwaitTimeMS: 500)
    end
    doc = resp.replies.first.documents.first

    elapsed_time.should > 0.5
  end
end
