# frozen_string_literal: true

require 'mongo'
require 'spec_helper'

describe Mongo::Crypt::EncryptionIO do
  let(:subject) do
    described_class.new(
      key_vault_namespace: 'foo.bar',
      key_vault_client: authorized_client,
      metadata_client: authorized_client.with(auto_encryption_options: nil),
      mongocryptd_options: mongocryptd_options
    )
  end

  describe '#spawn_mongocryptd' do
    context 'no spawn path' do
      let(:mongocryptd_options) do
        {
          mongocryptd_spawn_args: [ 'test' ],
        }
      end

      it 'fails with an exception' do
        lambda do
          subject.send(:spawn_mongocryptd)
        end.should raise_error(ArgumentError, /Cannot spawn mongocryptd process when no.*mongocryptd_spawn_path/)
      end
    end

    context 'no spawn args' do
      let(:mongocryptd_options) do
        {
          mongocryptd_spawn_path: 'echo',
        }
      end

      it 'fails with an exception' do
        lambda do
          subject.send(:spawn_mongocryptd)
        end.should raise_error(ArgumentError, /Cannot spawn mongocryptd process when no.*mongocryptd_spawn_args/)
      end
    end

    context 'empty array for spawn args' do
      let(:mongocryptd_options) do
        {
          mongocryptd_spawn_path: 'echo',
          mongocryptd_spawn_args: [],
        }
      end

      it 'fails with an exception' do
        lambda do
          subject.send(:spawn_mongocryptd)
        end.should raise_error(ArgumentError, /Cannot spawn mongocryptd process when no.*mongocryptd_spawn_args/)
      end
    end

    context 'good spawn path and args' do
      let(:mongocryptd_options) do
        {
          mongocryptd_spawn_path: 'echo',
          mongocryptd_spawn_args: [ 'hi' ],
        }
      end

      it 'spawns' do
        subject.send(:spawn_mongocryptd)
      end
    end

    context '-- for args to emulate no args' do
      let(:mongocryptd_options) do
        {
          mongocryptd_spawn_path: 'echo',
          mongocryptd_spawn_args: [ '--' ],
        }
      end

      it 'spawns' do
        subject.send(:spawn_mongocryptd)
      end
    end
  end

  describe '#with_ssl_socket' do
    context 'when a kms_connect_callback is provided' do
      let(:kms_connect_callback) { double('callback', call: nil) }

      let(:subject) do
        described_class.new(
          key_vault_namespace: 'foo.bar',
          key_vault_client: authorized_client,
          metadata_client: authorized_client.with(auto_encryption_options: nil),
          mongocryptd_options: {},
          kms_connect_callback: kms_connect_callback
        )
      end

      it 'invokes the callback with host, port, and timeout_ms' do
        expect(kms_connect_callback).to receive(:call).with('kms.example.com', 443, 5000).and_raise(IOError, 'boom')

        expect do
          subject.send(:with_ssl_socket, 'kms.example.com:443', {}, timeout_ms: 5000) { |s| s }
        end.to raise_error(Mongo::Error::KmsError, /boom/)
      end

      it 'treats callback failures as transient' do
        allow(kms_connect_callback).to receive(:call).and_raise(IOError, 'boom')

        expect do
          subject.send(:with_ssl_socket, 'kms.example.com:443', {}, timeout_ms: 5000) { |s| s }
        end.to raise_error(Mongo::Error::KmsError) { |e| expect(e.network_error?).to be true }
      end

      it 'raises KmsError when the callback returns nil' do
        expect(kms_connect_callback).to receive(:call)
          .with('kms.example.com', 443, nil).and_return(nil)

        expect do
          subject.send(:with_ssl_socket, 'kms.example.com:443', {}) { |s| s }
        end.to raise_error(Mongo::Error::KmsError, /must return an IO/)
      end

      it 'raises KmsError when the callback returns a non-IO value' do
        allow(kms_connect_callback).to receive(:call).and_return('not a socket')

        expect do
          subject.send(:with_ssl_socket, 'kms.example.com:443', {}, timeout_ms: 5000) { |s| s }
        end.to raise_error(Mongo::Error::KmsError, /must return an IO/)
      end

      it 'gives the handshake the driver connect budget when no budget is given' do
        connected_socket = double('connected socket', to_io: nil)
        mongo_socket = double('mongo socket', socket: nil, close: nil)
        allow(kms_connect_callback).to receive(:call).and_return(connected_socket)
        allow(Mongo::Socket::SSL).to receive(:new).and_return(mongo_socket)

        expect(Mongo::Socket::SSL).to receive(:new) do |*args|
          expect(args.last[:connect_timeout]).to be_within(0.5).of(Mongo::Server::CONNECT_TIMEOUT)
          mongo_socket
        end

        subject.send(:with_ssl_socket, 'kms.example.com:443', {}) { |s| s }
      end

      it 'uses the remaining budget as connect_timeout when CSOT is in effect' do
        connected_socket = double('connected socket', to_io: nil)
        mongo_socket = double('mongo socket', socket: nil, close: nil)
        allow(kms_connect_callback).to receive(:call).and_return(connected_socket)
        allow(Mongo::Socket::SSL).to receive(:new).and_return(mongo_socket)

        expect(Mongo::Socket::SSL).to receive(:new) do |*args|
          expect(args.last[:connect_timeout]).to be_within(0.5).of(5.0)
          mongo_socket
        end

        subject.send(:with_ssl_socket, 'kms.example.com:443', {}, timeout_ms: 5000) { |s| s }
      end

      it 'gives the handshake only the connect budget the callback left' do
        connected_socket = double('connected socket', to_io: nil)
        mongo_socket = double('mongo socket', socket: nil, close: nil)
        allow(kms_connect_callback).to receive(:call) do
          sleep 0.2
          connected_socket
        end
        allow(Mongo::Socket::SSL).to receive(:new).and_return(mongo_socket)

        expect(Mongo::Socket::SSL).to receive(:new) do |*args|
          # The callback already spent ~0.2s of the 5s budget, so the handshake
          # must get less than the full budget rather than a fresh 5 seconds.
          expect(args.last[:connect_timeout]).to be < 5.0
          expect(args.last[:connect_timeout]).to be > 4.0
          mongo_socket
        end

        subject.send(:with_ssl_socket, 'kms.example.com:443', {}, timeout_ms: 5000) { |s| s }
      end

      it 'bounds the callback by the remaining budget when CSOT is in effect' do
        connected_socket = double('connected socket', to_io: nil)
        mongo_socket = double('mongo socket', socket: nil, close: nil)
        allow(Mongo::Socket::SSL).to receive(:new).and_return(mongo_socket)
        allow(kms_connect_callback).to receive(:call) do
          sleep 0.5
          connected_socket
        end

        started = Mongo::Utils.monotonic_time

        expect do
          subject.send(:with_ssl_socket, 'kms.example.com:443', {}, timeout_ms: 50) { |s| s }
        end.to raise_error(Mongo::Error::KmsError, /TimeoutError/)

        # A 50 millisecond budget must fire well under a second; without the
        # bound the callback's 0.5 second sleep would complete first.
        expect(Mongo::Utils.monotonic_time - started).to be < 1
      end

      it 'bounds the callback by the driver connect timeout when no budget is given' do
        stub_const('Mongo::Server::CONNECT_TIMEOUT', 0.05)
        connected_socket = double('connected socket', to_io: nil)
        mongo_socket = double('mongo socket', socket: nil, close: nil)
        allow(Mongo::Socket::SSL).to receive(:new).and_return(mongo_socket)
        allow(kms_connect_callback).to receive(:call) do
          sleep 0.5
          connected_socket
        end

        started = Mongo::Utils.monotonic_time

        expect do
          subject.send(:with_ssl_socket, 'kms.example.com:443', {}) { |s| s }
        end.to raise_error(Mongo::Error::KmsError, /TimeoutError/)

        expect(Mongo::Utils.monotonic_time - started).to be < 1
      end

      it 'does not treat a callback timeout as a retryable network error' do
        allow(kms_connect_callback).to receive(:call) do
          sleep 0.5
          double('connected socket', to_io: nil)
        end

        expect do
          subject.send(:with_ssl_socket, 'kms.example.com:443', {}, timeout_ms: 50) { |s| s }
        end.to raise_error(Mongo::Error::KmsError) { |e| expect(e.network_error?).to be false }
      end
    end

    context 'when a non-callable kms_connect_callback is provided' do
      it 'raises ArgumentError' do
        expect do
          described_class.new(
            key_vault_namespace: 'foo.bar',
            key_vault_client: authorized_client,
            metadata_client: authorized_client.with(auto_encryption_options: nil),
            mongocryptd_options: {},
            kms_connect_callback: 'not callable'
          )
        end.to raise_error(ArgumentError, /must respond to :call/)
      end
    end
  end

  describe '#feed_kms' do
    let(:mongocryptd_options) { {} }

    let(:kms_connect_callback) { double('callback') }
    let(:connected_socket) { double('connected socket', to_io: nil) }
    let(:mongo_socket) { double('mongo socket') }

    let(:subject) do
      described_class.new(
        key_vault_namespace: 'foo.bar',
        key_vault_client: authorized_client,
        metadata_client: authorized_client.with(auto_encryption_options: nil),
        mongocryptd_options: mongocryptd_options,
        kms_connect_callback: kms_connect_callback
      )
    end

    let(:kms_context) do
      double('kms_context',
             endpoint: 'kms.example.com:443',
             message: 'hello',
             bytes_needed: 0,
             feed: nil)
    end

    # A socket whose write blocks for longer than the CSOT budget we pass in,
    # so that the timeout is the only thing that can end the operation early.
    let(:blocking_socket) do
      double('socket').tap do |sock|
        allow(sock).to receive(:syswrite) do
          sleep 0.2
          raise IOError, 'syswrite should have been interrupted by the timeout'
        end
      end
    end

    before do
      # Exercise the real with_ssl_socket wrapper by stubbing only the socket
      # it would construct, so the timeout is observed exactly as in production.
      allow(kms_connect_callback).to receive(:call).and_return(connected_socket)
      allow(Mongo::Socket::SSL).to receive(:new).and_return(mongo_socket)
      allow(mongo_socket).to receive(:socket).and_return(blocking_socket)
      allow(mongo_socket).to receive(:close)
    end

    it 'converts the millisecond budget to seconds and applies it' do
      started = Mongo::Utils.monotonic_time

      expect do
        subject.feed_kms(kms_context, {}, timeout_ms: 50)
      end.to raise_error(Mongo::Error::KmsError,
                         /Mongo::Error::TimeoutError: Socket write operation timed out/)

      # A 50 millisecond budget must fire well under a second. If the value
      # were passed to Timeout.timeout as seconds, the 0.2 second write would
      # complete first and raise IOError instead.
      expect(Mongo::Utils.monotonic_time - started).to be < 1
    end

    it 'does not mark a timeout as a retryable network error' do
      expect do
        subject.feed_kms(kms_context, {}, timeout_ms: 50)
      end.to raise_error(Mongo::Error::KmsError) { |e| expect(e.network_error?).to be false }
    end

    it 'uses the fixed SOCKET_TIMEOUT seconds when no budget is given' do
      allow(blocking_socket).to receive(:syswrite) do
        sleep 0.2
        nil
      end

      # A 0.2 second write must not time out when the fallback is 10 seconds,
      # proving the non-CSOT bound was left in seconds and unchanged.
      expect do
        subject.feed_kms(kms_context, {})
      end.not_to raise_error
    end

    it 'bounds a chunked response by a single shared budget' do
      # Three chunks, each stalling for 120ms, under a 200ms budget.
      needed = [ 1, 1, 1, 0 ]
      allow(kms_context).to receive(:bytes_needed) { needed.shift }
      allow(blocking_socket).to receive(:syswrite).and_return(nil)
      allow(blocking_socket).to receive(:sysread) do
        sleep 0.12
        'x'
      end

      started = Mongo::Utils.monotonic_time

      # If each phase restarted the full budget the exchange would complete
      # after ~360ms; sharing one deadline must time it out at ~200ms.
      expect do
        subject.feed_kms(kms_context, {}, timeout_ms: 200)
      end.to raise_error(Mongo::Error::KmsError, /TimeoutError/)

      expect(Mongo::Utils.monotonic_time - started).to be < 1
    end
  end

  describe '#remaining_timeout' do
    let(:subject) do
      described_class.new(
        key_vault_namespace: 'foo.bar',
        key_vault_client: authorized_client,
        metadata_client: authorized_client.with(auto_encryption_options: nil),
        mongocryptd_options: {}
      )
    end

    it 'falls back to SOCKET_TIMEOUT when there is no deadline' do
      expect(subject.send(:remaining_timeout, nil)).to eq(described_class::SOCKET_TIMEOUT)
    end

    it 'returns the seconds left before the deadline' do
      deadline = Mongo::Utils.monotonic_time + 5

      expect(subject.send(:remaining_timeout, deadline)).to be_within(0.5).of(5)
    end

    it 'raises TimeoutError once the deadline has passed' do
      deadline = Mongo::Utils.monotonic_time - 1

      expect do
        subject.send(:remaining_timeout, deadline)
      end.to raise_error(Mongo::Error::TimeoutError)
    end
  end

  describe '#collection_info' do
    let(:subject) do
      described_class.new(
        key_vault_namespace: 'foo.bar',
        key_vault_client: authorized_client,
        metadata_client: authorized_client,
        mongocryptd_options: {}
      )
    end

    let(:db_name) { 'collection_info_test' }
    let(:test_client) { authorized_client }

    before do
      test_client.use(db_name)['col_a'].drop
      test_client.use(db_name)['col_b'].drop
      test_client.use(db_name).command(create: 'col_a')
      test_client.use(db_name).command(create: 'col_b')
    end

    after do
      test_client.use(db_name)['col_a'].drop
      test_client.use(db_name)['col_b'].drop
    end

    it 'returns an array of all matching collection documents' do
      results = subject.collection_info(db_name, {}, timeout_ms: nil)
      names = results.map { |doc| doc['name'] }
      expect(names).to include('col_a', 'col_b')
    end

    it 'returns an empty array when no collections match' do
      results = subject.collection_info(db_name, { name: 'nonexistent' }, timeout_ms: nil)
      expect(results).to eq([])
    end

    it 'returns an array even when only one collection matches' do
      results = subject.collection_info(db_name, { name: 'col_a' }, timeout_ms: nil)
      expect(results).to be_an(Array)
      expect(results.length).to eq(1)
      expect(results.first['name']).to eq('col_a')
    end
  end

  describe '#mark_command' do
    let(:mock_client) do
      double('mongocryptd client').tap do |client|
        database = double('mock database')
        expect(database).to receive(:command).and_raise(Mongo::Error::NoServerAvailable.new(
                                                          Mongo::ServerSelector::Primary.new, nil, 'test message'
                                                        ))
        allow(database).to receive(:command).and_return([])
        expect(client).to receive(:database).at_least(:once).and_return(database)
      end
    end

    let(:base_options) do
      {
        mongocryptd_spawn_path: 'echo',
        mongocryptd_spawn_args: [ '--' ],
      }
    end

    let(:subject) do
      described_class.new(
        mongocryptd_client: mock_client,
        key_vault_namespace: 'foo.bar',
        key_vault_client: authorized_client,
        metadata_client: authorized_client.with(auto_encryption_options: nil),
        mongocryptd_options: mongocryptd_options
      )
    end

    context ':mongocryptd_bypass_spawn not given' do
      let(:mongocryptd_options) do
        base_options
      end

      it 'spawns' do
        expect(subject).to receive(:spawn_mongocryptd)
        subject.mark_command({})
      end
    end

    context ':mongocryptd_bypass_spawn given' do
      let(:mongocryptd_options) do
        base_options.merge(
          mongocryptd_bypass_spawn: true
        )
      end

      it 'does not spawn' do
        expect(subject).not_to receive(:spawn_mongocryptd)
        lambda do
          subject.mark_command({})
        end.should raise_error(Mongo::Error::NoServerAvailable, /test message/)
      end
    end
  end
end
