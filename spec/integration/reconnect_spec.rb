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
end
