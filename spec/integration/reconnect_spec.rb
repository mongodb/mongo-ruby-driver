require 'spec_helper'

describe 'Client after reconnect' do
  let(:client) { authorized_client }

  it 'works' do
    client['test'].insert_one('testk' => 'testv')

    client.reconnect

    doc = client['test'].find('testk' => 'testv').first
    expect(doc).not_to be_nil
    expect(doc['testk']).to eq('testv')
  end

  it 'recreates monitor thread' do
    thread = client.cluster.servers.first.monitor.instance_variable_get('@thread')
    expect(thread).to be_alive

    thread.kill
    # context switch to let the thread get killed
    sleep 0.1
    expect(thread).not_to be_alive

    client.reconnect

    new_thread = client.cluster.servers.first.monitor.instance_variable_get('@thread')
    expect(new_thread).not_to eq(thread)
    expect(new_thread).to be_alive
  end
end
