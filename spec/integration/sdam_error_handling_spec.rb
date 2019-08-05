require 'spec_helper'

describe 'SDAM error handling' do
  before(:all) do
    ClientRegistry.instance.close_all_clients
  end

  # These tests operate on specific servers, and don't work in a multi
  # shard cluster where multiple servers are equally eligible
  require_no_multi_shard

  let(:client) { authorized_client_without_any_retries }

  let(:server) { client.cluster.next_primary }

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

  shared_examples_for 'clears connection pool' do
    it 'clears connection pool' do
      generation = server.pool.generation
      operation
      new_generation = server.pool.generation
      # Temporary hack to allow repeated pool clears
      expect(new_generation).to be >= generation + 1
    end
  end

  shared_examples_for 'does not clear connection pool' do
    it 'does not clear connection pool' do
      generation = server.pool.generation
      operation
      new_generation = server.pool.generation
      expect(new_generation).to eq(generation)
    end
  end

  describe 'when there is an error during an operation' do

    before do
      wait_for_all_servers(client.cluster)
      # we also need a connection to the primary so that our error
      # expectations do not get triggered during handshakes which
      # have different behavior from non-handshake errors
      client.database.command(ping: 1)
      client.cluster.servers_list.each do |server|
        server.monitor.stop!
      end
    end

    let(:operation) do
      expect_any_instance_of(Mongo::Server::Connection).to receive(:deliver).and_return(reply)
      expect do
        client.database.command(ping: 1)
      end.to raise_error(Mongo::Error::OperationFailure, exception_message)
    end

    shared_examples_for 'not master or node recovering' do
      it_behaves_like 'marks server unknown'
      it_behaves_like 'requests server scan'

      context 'server 4.2 or higher' do
        min_server_fcv '4.2'

        # Due to RUBY-1894 backport, the pool is cleared here
        it_behaves_like 'clears connection pool'
      end

      context 'server 4.0 or lower' do
        max_server_version '4.0'

        it_behaves_like 'clears connection pool'
      end
    end

    shared_examples_for 'node shutting down' do
      it_behaves_like 'marks server unknown'
      it_behaves_like 'requests server scan'
      it_behaves_like 'clears connection pool'
    end

    context 'not master error' do
      let(:exception_message) do
        /not master/
      end

      let(:reply) do
        make_not_master_reply
      end

      it_behaves_like 'not master or node recovering'
    end

    context 'node recovering error' do
      let(:exception_message) do
        /DueToStepDown/
      end

      let(:reply) do
        make_node_recovering_reply
      end

      it_behaves_like 'not master or node recovering'
    end

    context 'node shutting down error' do
      let(:exception_message) do
        /shutdown in progress/
      end

      let(:reply) do
        make_node_shutting_down_reply
      end

      it_behaves_like 'node shutting down'
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
        it_behaves_like 'clears connection pool'
      end

      context 'network timeout error' do
        let(:exception) do
          Mongo::Error::SocketTimeoutError
        end

        it_behaves_like 'does not mark server unknown'
        it_behaves_like 'does not request server scan'
        it_behaves_like 'does not clear connection pool'
      end
    end
  end

  # These tests fail intermittently in Evergreen
  describe 'when there is an error on monitoring connection', retry: 3 do
    let(:client) do
      authorized_client_without_any_retries.with(
        connect_timeout: 1, socket_timeout: 1)
    end

    let(:operation) do
      expect(server.monitor.connection).not_to be nil
      expect(server.monitor.connection).to receive(:ismaster).at_least(:once).and_raise(exception)
      server.monitor.scan_semaphore.broadcast
      6.times do
        sleep 0.5
        if server.unknown?
          break
        end
      end
      expect(server).to be_unknown
    end

    context 'non-timeout network error' do
      let(:exception) { Mongo::Error::SocketError }

      it_behaves_like 'marks server unknown'
      it_behaves_like 'clears connection pool'
    end

    context 'network timeout' do
      let(:exception) { Mongo::Error::SocketTimeoutError }

      it_behaves_like 'marks server unknown'
      it_behaves_like 'clears connection pool'
    end
  end
end
