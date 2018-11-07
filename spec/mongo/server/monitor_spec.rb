require 'spec_helper'

describe Mongo::Server::Monitor do
  before(:all) do
    ClientRegistry.instance.close_all_clients
  end

  let(:address) do
    default_address
  end

  let(:listeners) do
    Mongo::Event::Listeners.new
  end

  describe '#scan!' do

    context 'when calling multiple times in succession' do

      let(:monitor) do
        described_class.new(address, listeners, Mongo::Monitoring.new,
          SpecConfig.instance.test_options)
      end

      it 'throttles the scans to minimum 500ms' do
        start = Time.now
        monitor.scan!
        monitor.scan!
        expect(Time.now - start).to be >= 0.5
      end
    end

    context 'when the ismaster fails the first time' do

      let(:monitor) do
        described_class.new(address, listeners, Mongo::Monitoring.new,
          SpecConfig.instance.test_options.merge(monitoring_io: false))
      end

      let(:socket) do
        monitor.connection.connect!
        monitor.connection.__send__(:socket)
      end

      before do
        expect(socket).to receive(:write).once.and_raise(Mongo::Error::SocketError)
        expect(socket).to receive(:write).and_call_original
        monitor.scan!
      end

      context 'in single topology' do
        require_topology :single

        it 'retries the ismaster' do
          expect(monitor.description).to be_standalone
        end
      end

      context 'in replica set topology' do
        require_topology :replica_set

        it 'retries the ismaster' do
          expect(monitor.description).to be_primary
        end
      end

      context 'in sharded topology' do
        require_topology :sharded

        it 'retries the ismaster' do
          expect(monitor.description).to be_mongos
        end
      end
    end

    context 'when the ismaster command succeeds' do

      let(:monitor) do
        described_class.new(address, listeners, Mongo::Monitoring.new,
          SpecConfig.instance.test_options)
      end

      before do
        monitor.scan!
      end

      context 'in single topology' do
        require_topology :single

        it 'updates the server description' do
          expect(monitor.description).to be_standalone
        end
      end

      context 'in replica set topology' do
        require_topology :replica_set

        it 'updates the server description' do
          expect(monitor.description).to be_primary
        end
      end

      context 'in sharded topology' do
        require_topology :sharded

        it 'updates the server description' do
          expect(monitor.description).to be_mongos
        end
      end
    end

    context 'when the ismaster command fails' do

      context 'when no server is running on the address' do

        let(:bad_address) do
          Mongo::Address.new('127.0.0.1:27050')
        end

        let(:monitor) do
          described_class.new(bad_address, listeners, Mongo::Monitoring.new)
        end

        before do
          monitor.scan!
        end

        it 'keeps the server unknown' do
          expect(monitor.description).to be_unknown
        end
      end

      context 'when the socket gets an exception' do

        let(:bad_address) do
          default_address
        end

        let(:monitor) do
          described_class.new(bad_address, listeners, Mongo::Monitoring.new)
        end

        let(:socket) do
          monitor.connection.connect!
          monitor.connection.__send__(:socket)
        end

        before do
          expect(socket).to receive(:write).twice.and_raise(Mongo::Error::SocketError)
          monitor.scan!
        end

        it 'keeps the server unknown' do
          expect(monitor.description).to be_unknown
        end

        it 'disconnects the connection' do
          expect(monitor.connection).to_not be_connected
        end
      end
    end
  end

  describe '#heartbeat_frequency' do

    context 'when an option is provided' do

      let(:monitor) do
        described_class.new(address, listeners, Mongo::Monitoring.new, :heartbeat_frequency => 5)
      end

      it 'returns the option' do
        expect(monitor.heartbeat_frequency).to eq(5)
      end
    end

    context 'when no option is provided' do

      let(:monitor) do
        described_class.new(address, listeners, Mongo::Monitoring.new)
      end

      it 'defaults to 10' do
        expect(monitor.heartbeat_frequency).to eq(10)
      end
    end
  end

  describe '#run!' do

    let(:monitor) do
      described_class.new(address, listeners, Mongo::Monitoring.new, :heartbeat_frequency => 1)
    end

    before do
      monitor.run!
      sleep(1)
    end

    it 'refreshes the server on the provided interval' do
      expect(monitor.description).to_not be_nil
    end
  end

  describe '#restart!' do

    let(:monitor) do
      described_class.new(address, listeners, Mongo::Monitoring.new, SpecConfig.instance.test_options)
    end

    let!(:thread) do
      monitor.run!
    end

    context 'when the monitor is already running' do

      it 'does not create a new thread' do
        expect(monitor.restart!).to be(thread)
      end
    end

    context 'when the monitor is not already running' do

      before do
        monitor.stop!
        sleep(1)
      end

      it 'creates a new thread' do
        expect(monitor.restart!).not_to be(thread)
      end
    end
  end

  # fails intermittently on jruby in evergreen
  describe '#stop', retry: 3 do

    let(:monitor) do
      described_class.new(address, listeners, Mongo::Monitoring.new,
        SpecConfig.instance.test_options)
    end

    let!(:thread) do
      monitor.run!
    end

    before do
      expect(monitor.connection).to receive(:disconnect!).and_call_original
      monitor.stop!
      sleep(1)
    end

    it 'kills the monitor thread' do
      expect(thread.alive?).to be(false)
    end
  end

  describe '#connection' do

    context 'when there is a connect_timeout option set' do

      let(:connect_timeout) do
        1
      end

      let(:monitor) do
        described_class.new(address, listeners, Mongo::Monitoring.new, SpecConfig.instance.test_options.merge(connect_timeout: connect_timeout))
      end

      it 'sets the value as the timeout on the connection' do
        expect(monitor.connection.timeout).to eq(connect_timeout)
      end

      it 'set the value as the timeout on the socket' do
        monitor.connection.connect!
        expect(monitor.connection.send(:socket).timeout).to eq(connect_timeout)
      end
    end
  end

  describe '#round_trip_times' do
    context 'no existing average rtt' do
      let(:monitor) do
        described_class.new(address, listeners, Mongo::Monitoring.new, SpecConfig.instance.test_options)
      end

      it 'returns last rtt' do
        expect(monitor).to receive(:round_trip_time).and_return(5)
        expect(monitor.send(:round_trip_times, Time.now)).to eq([5, 5])
      end
    end

    context 'with existing average rtt' do
      let(:monitor) do
        described_class.new(address, listeners, Mongo::Monitoring.new, SpecConfig.instance.test_options)
      end

      it 'averages with existing average rtt' do
        monitor.send(:instance_variable_set, '@average_round_trip_time', 10)
        expect(monitor).to receive(:round_trip_time).and_return(5)
        expect(monitor.send(:round_trip_times, Time.now)).to eq([5, 9])
      end
    end
  end
end
