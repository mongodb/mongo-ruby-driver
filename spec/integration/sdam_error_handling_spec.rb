require 'spec_helper'

describe 'SDAM error handling' do
  clean_slate_for_all

  after do
    # Close all clients after every test to avoid leaking expectations into
    # subsequent tests because we set global assertions on sockets.
    ClientRegistry.instance.close_all_clients
  end

  # These tests operate on specific servers, and don't work in a multi
  # shard cluster where multiple servers are equally eligible
  require_no_multi_shard

  let(:diagnostic_subscriber) { VerboseEventSubscriber.new }

  let(:client) do
    new_local_client(SpecConfig.instance.addresses,
      SpecConfig.instance.all_test_options.merge(
        socket_timeout: 3, connect_timeout: 3,
        # Uncomment to print all events to stdout:
        #sdam_proc: Utils.subscribe_all_sdam_proc(diagnostic_subscriber),
        **Utils.disable_retries_client_options)
    )
  end

  let(:server) { client.cluster.next_primary }

  shared_examples_for 'marks server unknown' do
    it 'marks server unknown' do
      expect(server).not_to be_unknown
      RSpec::Mocks.with_temporary_scope do
        operation
        expect(server).to be_unknown
      end
    end
  end

  shared_examples_for 'does not mark server unknown' do
    it 'does not mark server unknown' do
      expect(server).not_to be_unknown
      RSpec::Mocks.with_temporary_scope do
        operation
        expect(server).not_to be_unknown
      end
    end
  end

  shared_examples_for 'requests server scan' do
    it 'requests server scan' do
      RSpec::Mocks.with_temporary_scope do
        expect(server.scan_semaphore).to receive(:signal)
        operation
      end
    end
  end

  shared_examples_for 'does not request server scan' do
    it 'does not request server scan' do
      RSpec::Mocks.with_temporary_scope do
        expect(server.scan_semaphore).not_to receive(:signal)
        operation
      end
    end
  end

  shared_examples_for 'clears connection pool' do
    it 'clears connection pool' do
      generation = server.pool.generation
      RSpec::Mocks.with_temporary_scope do
        operation
        new_generation = server.pool.generation
        expect(new_generation).to eq(generation + 1)
      end
    end
  end

  shared_examples_for 'does not clear connection pool' do
    it 'does not clear connection pool' do
      generation = server.pool.generation
      RSpec::Mocks.with_temporary_scope do
        operation
        new_generation = server.pool.generation
        expect(new_generation).to eq(generation)
      end
    end
  end

  describe 'when there is an error during an operation' do

    before do
      client.cluster.next_primary
      # we also need a connection to the primary so that our error
      # expectations do not get triggered during handshakes which
      # have different behavior from non-handshake errors
      client.database.command(ping: 1)
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

        it_behaves_like 'does not clear connection pool'
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
      # With 4.4 servers we set up two monitoring connections, hence global
      # socket expectations get hit twice.
      max_server_version '4.2'

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

  describe 'when there is an error on monitoring connection' do
    clean_slate_for_all

    let(:subscriber) { EventSubscriber.new }

    let(:set_subscribers) do
      client.subscribe(Mongo::Monitoring::SERVER_DESCRIPTION_CHANGED, subscriber)
      client.subscribe(Mongo::Monitoring::CONNECTION_POOL, subscriber)
    end

    let(:operation) do
      expect(server.monitor.connection).not_to be nil
      set_subscribers
      RSpec::Mocks.with_temporary_scope do
        expect(server.monitor).to receive(:ismaster).and_raise(exception)
        server.monitor.scan!
      end
      expect_server_state_change
    end

    shared_examples_for 'marks server unknown - sdam event' do
      it 'marks server unknown' do
        expect(server).not_to be_unknown

        #subscriber.clear_events!
        events = subscriber.select_succeeded_events(Mongo::Monitoring::Event::ServerDescriptionChanged)
        events.should be_empty

        RSpec::Mocks.with_temporary_scope do
          operation

          events = subscriber.select_succeeded_events(Mongo::Monitoring::Event::ServerDescriptionChanged)
          events.should_not be_empty
          event = events.detect do |event|
            event.new_description.address == server.address &&
            event.new_description.unknown?
          end
          event.should_not be_nil
        end
      end
    end

    shared_examples_for 'clears connection pool - cmap event' do
      it 'clears connection pool' do
        #subscriber.clear_events!
        events = subscriber.select_published_events(Mongo::Monitoring::Event::Cmap::PoolCleared)
        events.should be_empty

        RSpec::Mocks.with_temporary_scope do
          operation

          events = subscriber.select_published_events(Mongo::Monitoring::Event::Cmap::PoolCleared)
          events.should_not be_empty
          event = events.detect do |event|
            event.address == server.address
          end
          event.should_not be_nil
        end
      end
    end

    shared_examples_for 'marks server unknown and clears connection pool' do
=begin These tests are not reliable
      context 'via object inspection' do
        let(:expect_server_state_change) do
          server.summary.should =~ /unknown/i
          expect(server).to be_unknown
        end

        it_behaves_like 'marks server unknown'
        it_behaves_like 'clears connection pool'
      end
=end

      context 'via events' do
        # When we use events we do not need to examine object state, therefore
        # it does not matter whether the server stays unknown or gets
        # successfully checked.
        let(:expect_server_state_change) do
          # nothing
        end

        it_behaves_like 'marks server unknown - sdam event'
        it_behaves_like 'clears connection pool - cmap event'
      end
    end

    context 'via stubs' do
      # With 4.4 servers we set up two monitoring connections, hence global
      # socket expectations get hit twice.
      max_server_version '4.2'

      context 'network timeout' do
        let(:exception) { Mongo::Error::SocketTimeoutError }

        it_behaves_like 'marks server unknown and clears connection pool'
      end

      context 'non-timeout network error' do
        let(:exception) { Mongo::Error::SocketError }

        it_behaves_like 'marks server unknown and clears connection pool'
      end
    end

    context 'non-timeout network error via fail point' do
      require_fail_command

      let(:admin_client) { client.use(:admin) }

      let(:set_fail_point) do
        admin_client.command(
          configureFailPoint: 'failCommand',
          mode: {times: 2},
          data: {
            failCommands: %w(isMaster),
            closeConnection: true,
          },
        )
      end

      let(:operation) do
        expect(server.monitor.connection).not_to be nil
        set_subscribers
        set_fail_point
        server.monitor.scan!
        expect_server_state_change
      end

      it_behaves_like 'marks server unknown and clears connection pool'

      after do
        admin_client.command(configureFailPoint: 'failCommand', mode: 'off')
      end
    end
  end
end
