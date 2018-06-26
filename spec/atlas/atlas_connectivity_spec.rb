require 'lite_spec_helper'

describe 'Atlas connectivity' do
  shared_examples 'connects to Atlas' do
    let(:uri) { ENV[var] }
    let(:client) { Mongo::Client.new(uri) }

    before do
      if uri.nil?
        skip "#{var} not set in environment"
      end
    end

    it 'runs ismaster successfully' do
      result = client.database.command(:ismaster => 1)
      expect(result.documents.first['ismaster']).to be true
    end

    it 'runs findOne successfully' do
      result = client.use(:test)['test'].find.to_a
      expect(result).to be_a(Array)
    end
  end

  context 'Atlas replica set' do
    let(:var) { 'ATLAS_REPLICA_SET_URI' }

    it_behaves_like 'connects to Atlas'
  end

  context 'Atlas sharded cluster' do
    let(:var) { 'ATLAS_SHARDED_URI' }

    it_behaves_like 'connects to Atlas'
  end

  context 'Atlas free tier replica set' do
    let(:var) { 'ATLAS_FREE_TIER_URI' }

    it_behaves_like 'connects to Atlas'
  end

  context 'Atlas TLS 1.1 only replica set' do
    let(:var) { 'ATLAS_TLS11_URI' }

    it_behaves_like 'connects to Atlas'
  end

  context 'Atlas TLS 1.2 only replica set' do
    let(:var) { 'ATLAS_TLS12_URI' }

    it_behaves_like 'connects to Atlas'
  end
end
