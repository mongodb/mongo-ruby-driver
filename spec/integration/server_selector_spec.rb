require 'spec_helper'

describe 'Server selector' do
  let(:selector) { Mongo::ServerSelector::Primary.new }
  let(:client) { authorized_client }
  let(:cluster) { client.cluster }

  describe '#select_server' do
    let(:result) { selector.select_server(cluster) }

    it 'selects' do
      expect(result).to be_a(Mongo::Server)
    end

    context 'no servers in the cluster' do
      let(:client) { Mongo::Client.new([], server_selection_timeout: 2) }

      it 'raises NoServerAvailable' do
        expect do
          result
        end.to raise_error(Mongo::Error::NoServerAvailable, "Cluster has no addresses, and therefore will never have a server")
      end

      it 'does not wait for server selection timeout' do
        start_time = Time.now
        expect do
          result
        end.to raise_error(Mongo::Error::NoServerAvailable)
        time_passed = Time.now - start_time
        expect(time_passed < 1).to be true
      end
    end
  end
end
