require 'spec_helper'

describe 'awaited ismaster' do
  min_server_fcv '4.4'

  # If we send the consecutive ismasters to different mongoses,
  # they have different process ids, and so the awaited one would return
  # immediately.
  require_no_multi_shard

  let(:client) { authorized_client }

  it 'waits' do
    # Perform a regular ismaster to get topology version
    resp = client.database.command(ismaster: 1)
    doc = resp.replies.first.documents.first
    tv = Mongo::TopologyVersion.new(doc['topologyVersion'])
    tv.should be_a(BSON::Document)

    elapsed_time = Benchmark.realtime do
      resp = client.database.command(ismaster: 1,
        topologyVersion: tv.to_doc, maxAwaitTimeMS: 500)
    end
    doc = resp.replies.first.documents.first

    elapsed_time.should > 0.5
  end
end
