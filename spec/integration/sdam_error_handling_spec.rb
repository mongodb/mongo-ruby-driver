require 'spec_helper'

describe 'SDAM error handling' do
  before(:all) do
    ClientRegistry.instance.close_all_clients
  end

  describe 'when there is an error during an operation' do
    let(:client) { authorized_client }

    before do
      wait_for_all_servers(client.cluster)
      # we also need a connection to the primary so that our error
      # expectations do not get triggered during handshakes which
      # have different behavior from non-handshake errors
      client.database.command(ping: 1)
      client.cluster.servers_list.each do |server|
        server.monitor.stop!(true)
      end
    end

    let(:server) { client.cluster.next_primary }

    let(:operation) do
      expect_any_instance_of(Mongo::Server::Connection).to receive(:deliver).and_return(reply)
      expect do
        client.database.command(ping: 1)
      end.to raise_error(Mongo::Error::OperationFailure, exception_message)
    end

    shared_examples_for 'marks server unknown' do
      it 'marks server unknown' do
        expect(server).not_to be_unknown
        operation
        expect(server).to be_unknown
      end
    end

    shared_examples_for 'does not mark server unknown' do
      it 'does not mark server unknown' do
        expect(server).not_to be_unknown
        operation
        expect(server).not_to be_unknown
      end
    end

    shared_examples_for 'requests server scan' do
      it 'requests server scan' do
        expect(server.monitor.scan_semaphore).to receive(:signal)
        operation
      end
    end

    shared_examples_for 'does not request server scan' do
      it 'does not request server scan' do
        expect(server.monitor.scan_semaphore).not_to receive(:signal)
        operation
      end
    end

    context 'not master error' do
      let(:exception_message) do
        /not master/
      end

      let(:reply) do
        make_not_master_reply
      end

      it_behaves_like 'marks server unknown'
      it_behaves_like 'requests server scan'
    end

    context 'node is recovering error' do
      let(:exception_message) do
        /shutdown in progress/
      end

      let(:reply) do
        make_node_recovering_reply
      end

      it_behaves_like 'marks server unknown'
      it_behaves_like 'requests server scan'
    end

    context 'network error' do

      let(:operation) do
        expect_any_instance_of(Mongo::Socket).to receive(:read).and_raise(exception)
        expect do
          client.database.command(ping: 1)
        end.to raise_error(exception)
      end

      context 'non-timeout network error' do
        let(:exception) do
          Mongo::Error::SocketError
        end

        it_behaves_like 'marks server unknown'
        it_behaves_like 'does not request server scan'
      end

      context 'network timeout error' do
        let(:exception) do
          Mongo::Error::SocketTimeoutError
        end

        it_behaves_like 'does not mark server unknown'
        it_behaves_like 'does not request server scan'
      end
    end
  end
end
