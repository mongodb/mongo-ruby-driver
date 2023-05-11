# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

# This test is for checking connectivity of the test client to the
# test cluster. In other words, it is a test that the test suite is
# configured correctly.
describe 'Client connectivity' do
  shared_examples_for 'is correctly configured' do
    it 'is configured with the correct database' do
      expect(client.options[:database]).to eq(SpecConfig.instance.test_db)
    end

    it 'has correct database in the cluster' do
      expect(client.cluster.options[:database]).to eq(SpecConfig.instance.test_db)
    end
  end

  context 'no auth' do
    let(:client) { ClientRegistry.instance.global_client('basic') }

    it_behaves_like 'is correctly configured'

    it 'connects and is usable' do
      resp = client.database.command(ping: 1)
      expect(resp).to be_a(Mongo::Operation::Result)
    end
  end

  context 'with auth' do
    let(:client) { ClientRegistry.instance.global_client('authorized') }

    it_behaves_like 'is correctly configured'

    it 'connects and is usable' do
      client['connectivity_spec'].insert_one(foo: 1)
      expect(client['connectivity_spec'].find(foo: 1).first['foo']).to eq(1)
    end
  end
end
