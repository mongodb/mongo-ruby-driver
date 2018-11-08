require 'spec_helper'

describe 'Client connectivity' do
  context 'no auth' do
    let(:client) { ClientRegistry.instance.global_client('basic') }

    it 'connects and is usable' do
      resp = client.database.command(ismaster: 1)
      expect(resp).to be_a(Mongo::Operation::Result)
    end
  end

  context 'with auth' do
    let(:client) { ClientRegistry.instance.global_client('authorized') }

    it 'connects and is usable' do
      client['connectivity_spec'].insert_one(foo: 1)
      expect(client['connectivity_spec'].find(foo: 1).first['foo']).to eq(1)
    end
  end
end
