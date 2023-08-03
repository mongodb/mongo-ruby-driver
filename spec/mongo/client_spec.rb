# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

DEFAULT_LOCAL_HOST = '127.0.0.1:27017'
ALT_LOCAL_HOST = '127.0.0.1:27010'

# NB: tests for .new, #initialize, #use, #with and #dup are in
# client_construction_spec.rb.

describe Mongo::Client do
  let(:subscriber) { Mrss::EventSubscriber.new }

  let(:monitored_client) do
    root_authorized_client.tap do |client|
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    end
  end

  describe '#==' do
    let(:client) do
      new_local_client_nmio(
        [ DEFAULT_LOCAL_HOST ],
        read: { mode: :primary },
        database: SpecConfig.instance.test_db
      )
    end

    context 'when the other is a client' do
      context 'when the options and cluster are equal' do
        let(:other) do
          new_local_client_nmio(
            [ DEFAULT_LOCAL_HOST ],
            read: { mode: :primary },
            database: SpecConfig.instance.test_db
          )
        end

        it 'returns true' do
          expect(client).to eq(other)
        end
      end

      context 'when the options are not equal' do
        let(:other) do
          new_local_client_nmio(
            [ DEFAULT_LOCAL_HOST ],
            read: { mode: :secondary },
            database: SpecConfig.instance.test_db
          )
        end

        it 'returns false' do
          expect(client).not_to eq(other)
        end
      end

      context 'when cluster is not equal' do
        let(:other) do
          new_local_client_nmio(
            [ ALT_LOCAL_HOST ],
            read: { mode: :primary },
            database: SpecConfig.instance.test_db
          )
        end

        it 'returns false' do
          expect(client).not_to eq(other)
        end
      end
    end

    context 'when the other is not a client' do
      it 'returns false' do
        expect(client).not_to eq('test')
      end
    end
  end

  describe '#[]' do
    let(:client) do
      new_local_client_nmio([ DEFAULT_LOCAL_HOST ],
        database: SpecConfig.instance.test_db)
    end

    shared_examples_for 'a collection switching object' do
      before do
        client.use(:dbtest)
      end

      it 'returns the new collection' do
        expect(collection.name).to eq('users')
      end
    end

    context 'when provided a string' do
      let(:collection) do
        client['users']
      end

      it_behaves_like 'a collection switching object'
    end

    context 'when provided a symbol' do
      let(:collection) do
        client[:users]
      end

      it_behaves_like 'a collection switching object'
    end
  end

  describe '#eql' do
    let(:client) do
      new_local_client_nmio(
        [ DEFAULT_LOCAL_HOST ],
        read: { mode: :primary },
        database: SpecConfig.instance.test_db
      )
    end

    context 'when the other is a client' do
      context 'when the options and cluster are equal' do
        let(:other) do
          new_local_client_nmio(
            [ DEFAULT_LOCAL_HOST ],
            read: { mode: :primary },
            database: SpecConfig.instance.test_db
          )
        end

        it 'returns true' do
          expect(client).to eql(other)
        end
      end

      context 'when the options are not equal' do
        let(:other) do
          new_local_client_nmio(
            [ DEFAULT_LOCAL_HOST ],
            read: { mode: :secondary },
            database: SpecConfig.instance.test_db
          )
        end

        it 'returns false' do
          expect(client).not_to eql(other)
        end
      end

      context 'when the cluster is not equal' do
        let(:other) do
          new_local_client_nmio(
            [ ALT_LOCAL_HOST ],
            read: { mode: :primary },
            database: SpecConfig.instance.test_db
          )
        end

        it 'returns false' do
          expect(client).not_to eql(other)
        end
      end
    end

    context 'when the other is not a client' do
      let(:client) do
        new_local_client_nmio(
          [ DEFAULT_LOCAL_HOST ],
          read: { mode: :primary },
          database: SpecConfig.instance.test_db
        )
      end

      it 'returns false' do
        expect(client).not_to eql('test')
      end
    end
  end

  describe '#hash' do
    let(:client) do
      new_local_client_nmio(
        [ DEFAULT_LOCAL_HOST ],
        read: { mode: :primary },
        local_threshold: 0.010,
        server_selection_timeout: 10000,
        database: SpecConfig.instance.test_db
      )
    end

    let(:default_options) { Mongo::Options::Redacted.new(
      retry_writes: true, retry_reads: true, monitoring_io: false) }

    let(:options) do
      Mongo::Options::Redacted.new(read: { mode: :primary },
                                    local_threshold: 0.010,
                                    server_selection_timeout: 10000,
                                    database: SpecConfig.instance.test_db)
    end

    let(:expected) do
      [ client.cluster, default_options.merge(options) ].hash
    end

    it 'returns a hash of the cluster and options' do
      expect(client.hash).to eq(expected)
    end
  end

  describe '#inspect' do
    let(:client) do
      new_local_client_nmio(
        [ DEFAULT_LOCAL_HOST ],
        read: { mode: :primary },
        database: SpecConfig.instance.test_db
      )
    end

    it 'returns the cluster information' do
      expect(client.inspect).to match(/Cluster(.|\n)*topology=(.|\n)*servers=/)
    end

    context 'when there is sensitive data in the options' do
      let(:client) do
        new_local_client_nmio(
            [ DEFAULT_LOCAL_HOST ],
            read: { mode: :primary },
            database: SpecConfig.instance.test_db,
            password: 'some_password',
            user: 'emily'
        )
      end

      it 'does not print out sensitive data' do
        expect(client.inspect).not_to match('some_password')
      end
    end
  end

  describe '#server_selector' do
    context 'when there is a read preference set' do
      let(:client) do
        new_local_client_nmio([ DEFAULT_LOCAL_HOST ],
                            database: SpecConfig.instance.test_db,
                            read: mode,
                            server_selection_timeout: 2)
      end

      let(:server_selector) do
        client.server_selector
      end

      context 'when mode is primary' do
        let(:mode) do
          { mode: :primary }
        end

        it 'returns a primary server selector' do
          expect(server_selector).to be_a(Mongo::ServerSelector::Primary)
        end

        it 'passes the options to the cluster' do
          expect(client.cluster.options[:server_selection_timeout]).to eq(2)
        end
      end

      context 'when mode is primary_preferred' do
        let(:mode) do
          { mode: :primary_preferred }
        end

        it 'returns a primary preferred server selector' do
          expect(server_selector).to be_a(Mongo::ServerSelector::PrimaryPreferred)
        end
      end

      context 'when mode is secondary' do
        let(:mode) do
          { mode: :secondary }
        end

        it 'uses a Secondary server selector' do
          expect(server_selector).to be_a(Mongo::ServerSelector::Secondary)
        end
      end

      context 'when mode is secondary preferred' do
        let(:mode) do
          { mode: :secondary_preferred }
        end

        it 'uses a Secondary server selector' do
          expect(server_selector).to be_a(Mongo::ServerSelector::SecondaryPreferred)
        end
      end

      context 'when mode is nearest' do
        let(:mode) do
          { mode: :nearest }
        end

        it 'uses a Secondary server selector' do
          expect(server_selector).to be_a(Mongo::ServerSelector::Nearest)
        end
      end

      context 'when no mode provided' do
        let(:client) do
          new_local_client_nmio([ DEFAULT_LOCAL_HOST ],
                              database: SpecConfig.instance.test_db,
                              server_selection_timeout: 2)
        end

        it 'returns a primary server selector' do
          expect(server_selector).to be_a(Mongo::ServerSelector::Primary)
        end
      end

      context 'when the read preference is printed' do
        let(:client) do
          new_local_client_nmio(SpecConfig.instance.addresses, options)
        end

        let(:options) do
          { user: 'Emily', password: 'sensitive_data', server_selection_timeout: 0.1 }
        end

        before do
          allow(client.database.cluster).to receive(:single?).and_return(false)
        end

        let(:error) do
          begin
            client.database.command(ping: 1)
          rescue StandardError => e
            e
          end
        end

        it 'redacts sensitive client options' do
          expect(error.message).not_to match(options[:password])
        end
      end
    end
  end

  describe '#read_preference' do
    let(:client) do
      new_local_client_nmio([ DEFAULT_LOCAL_HOST ],
                          database: SpecConfig.instance.test_db,
                          read: mode,
                          server_selection_timeout: 2)
    end

    let(:preference) do
      client.read_preference
    end

    context 'when mode is primary' do
      let(:mode) do
        { mode: :primary }
      end

      it 'returns a primary read preference' do
        expect(preference).to eq(BSON::Document.new(mode))
      end
    end

    context 'when mode is primary_preferred' do
      let(:mode) do
        { mode: :primary_preferred }
      end

      it 'returns a primary preferred read preference' do
        expect(preference).to eq(BSON::Document.new(mode))
      end
    end

    context 'when mode is secondary' do
      let(:mode) do
        { mode: :secondary }
      end

      it 'returns a secondary read preference' do
        expect(preference).to eq(BSON::Document.new(mode))
      end
    end

    context 'when mode is secondary preferred' do
      let(:mode) do
        { mode: :secondary_preferred }
      end

      it 'returns a secondary preferred read preference' do
        expect(preference).to eq(BSON::Document.new(mode))
      end
    end

    context 'when mode is nearest' do
      let(:mode) do
        { mode: :nearest }
      end

      it 'returns a nearest read preference' do
        expect(preference).to eq(BSON::Document.new(mode))
      end
    end

    context 'when no mode provided' do
      let(:client) do
        new_local_client_nmio([ DEFAULT_LOCAL_HOST ],
                            database: SpecConfig.instance.test_db,
                            server_selection_timeout: 2)
      end

      it 'returns nil' do
        expect(preference).to be_nil
      end
    end
  end

  describe '#write_concern' do
    let(:concern) { client.write_concern }

    context 'when no option was provided to the client' do
      let(:client) { new_local_client_nmio([ DEFAULT_LOCAL_HOST ], database: SpecConfig.instance.test_db) }

      it 'does not set the write concern' do
        expect(concern).to be_nil
      end
    end

    context 'when an option is provided' do
      context 'when the option is acknowledged' do
        let(:client) do
          new_local_client_nmio([ DEFAULT_LOCAL_HOST ], write: { j: true }, database: SpecConfig.instance.test_db)
        end

        it 'returns a acknowledged write concern' do
          expect(concern.get_last_error).to eq(getlasterror: 1, j: true)
        end
      end

      context 'when the option is unacknowledged' do
        context 'when the w is 0' do
          let(:client) do
            new_local_client_nmio([ DEFAULT_LOCAL_HOST ], write: { w: 0 }, database: SpecConfig.instance.test_db)
          end

          it 'returns an unacknowledged write concern' do
            expect(concern.get_last_error).to be_nil
          end
        end

        context 'when the w is -1' do
          let(:client) do
            new_local_client_nmio([ DEFAULT_LOCAL_HOST ], write: { w: -1 }, database: SpecConfig.instance.test_db)
          end

          it 'raises an error' do
            expect {
              concern
            }.to raise_error(Mongo::Error::InvalidWriteConcern)
          end
        end
      end
    end
  end

  [
    [ :max_read_retries, 1 ],
    [ :read_retry_interval, 5 ],
    [ :max_write_retries, 1 ],
  ].each do |opt, default|
    describe "##{opt}" do
      let(:client_options) { {} }

      let(:client) do
        new_local_client_nmio([ DEFAULT_LOCAL_HOST ], client_options)
      end

      it "defaults to #{default}" do
        expect(default).not_to be nil
        expect(client.options[opt]).to be nil
        expect(client.send(opt)).to eq(default)
      end

      context 'specified on client' do
        let(:client_options) { { opt => 2 } }

        it 'inherits from client' do
          expect(client.options[opt]).to eq(2)
          expect(client.send(opt)).to eq(2)
        end
      end
    end
  end

  shared_context 'ensure test db exists' do
    before(:all) do
      # Ensure the database we are querying exists.
      # When the entire test suite is run, it will generally have been
      # created by a previous test, but if this test is run on a fresh
      # deployment the database won't exist.
      client = ClientRegistry.instance.global_client('authorized')
      client['any-collection-name'].insert_one(any: :value)
    end
  end

  describe '#database' do
    let(:database) { client.database }

    context 'when client has :server_api option' do
      let(:client) do
        new_local_client_nmio([ 'localhost' ], server_api: { version: '1' })
      end

      it 'is not transfered to the collection' do
        expect(database.options[:server_api]).to be_nil
      end
    end

  end

  describe '#database_names' do
    it 'returns a list of database names' do
      expect(root_authorized_client.database_names).to include(
        'admin'
      )
    end

    context 'when filter criteria is present' do
      min_server_fcv '3.6'

      include_context 'ensure test db exists'

      let(:result) do
        root_authorized_client.database_names(filter)
      end

      let(:filter) do
        { name: SpecConfig.instance.test_db }
      end

      it 'returns a filtered list of database names' do
        expect(result.length).to eq(1)
        expect(result.first).to eq(filter[:name])
      end
    end

    context 'with comment' do
      min_server_version '4.4'

      it 'returns a list of database names and send comment' do
        result = monitored_client.database_names({}, comment: 'comment')
        expect(result).to include('admin')
        command = subscriber.command_started_events('listDatabases').last&.command
        expect(command).not_to be_nil
        expect(command['comment']).to eq('comment')
      end
    end
  end

  describe '#list_databases' do
    it 'returns a list of database info documents' do
      expect(
        root_authorized_client.list_databases.collect do |i|
          i['name']
        end).to include('admin')
    end

    context 'when filter criteria is present' do
      min_server_fcv '3.6'

      include_context 'ensure test db exists'

      let(:result) do
        root_authorized_client.list_databases(filter)
      end

      let(:filter) do
        { name: SpecConfig.instance.test_db }
      end

      it 'returns a filtered list of database info documents' do
        expect(result.length).to eq(1)
        expect(result[0]['name']).to eq(filter[:name])
      end
    end

    context 'when name_only is true' do
      min_server_fcv '3.6'

      let(:command) do
        Utils.get_command_event(root_authorized_client, 'listDatabases') do |client|
          client.list_databases({}, true)
        end.command
      end

      it 'sends the command with the nameOnly flag set to true' do
        expect(command[:nameOnly]).to be(true)
      end
    end

    context 'when authorized_databases is provided' do
      min_server_fcv '4.0'

      let(:client_options) do
        root_authorized_client.options.merge(heartbeat_frequency: 100, monitoring: true)
      end

      let(:subscriber) { Mrss::EventSubscriber.new }

      let(:client) do
        ClientRegistry.instance.new_local_client(
          SpecConfig.instance.addresses, client_options
        ).tap do |cl|
          cl.subscribe(Mongo::Monitoring::COMMAND, subscriber)
        end
      end

      let(:command) do
        subscriber.started_events.find { |c| c.command_name == 'listDatabases' }.command
      end

      let(:authDb) do
        { authorized_databases: true }
      end

      let(:noAuthDb) do
        { authorized_databases: false }
      end

      before do
        client.list_databases({}, true, authDb)
        client.list_databases({}, true, noAuthDb)
      end

      let(:events) do
        subscriber.command_started_events('listDatabases')
      end

      it 'sends the command with the authorizedDatabases flag set to true' do
        expect(events.length).to eq(2)
        command = events.first.command
        expect(command[:authorizedDatabases]).to be(true)
      end

      it 'sends the command with the authorizedDatabases flag set to nil' do
        command = events.last.command
        expect(command[:authorizedDatabases]).to be_nil
      end
    end

    context 'with comment' do
      min_server_version '4.4'

      it 'returns a list of database names and send comment' do
        result = monitored_client.list_databases({}, false, comment: 'comment').collect do |i|
          i['name']
        end
        expect(result).to include('admin')
        command = subscriber.command_started_events('listDatabases').last&.command
        expect(command).not_to be_nil
        expect(command['comment']).to eq('comment')
      end
    end
  end

  describe '#list_mongo_databases' do
    let(:options) do
      { read: { mode: :secondary } }
    end

    let(:client) do
      root_authorized_client.with(options)
    end

    let(:result) do
      client.list_mongo_databases
    end

    it 'returns a list of Mongo::Database objects' do
      expect(result).to all(be_a(Mongo::Database))
    end

    it 'creates database with specified options' do
      expect(result.first.options[:read]).to eq(BSON::Document.new(options)[:read])
    end

    context 'when filter criteria is present' do
      min_server_fcv '3.6'

      include_context 'ensure test db exists'

      let(:result) do
        client.list_mongo_databases(filter)
      end

      let(:filter) do
        { name: SpecConfig.instance.test_db }
      end

      it 'returns a filtered list of Mongo::Database objects' do
        expect(result.length).to eq(1)
        expect(result.first.name).to eq(filter[:name])
      end
    end

    context 'with comment' do
      min_server_version '4.4'

      it 'returns a list of database names and send comment' do
        result = monitored_client.list_mongo_databases({}, comment: 'comment')
        expect(result).to all(be_a(Mongo::Database))
        command = subscriber.command_started_events('listDatabases').last&.command
        expect(command).not_to be_nil
        expect(command['comment']).to eq('comment')
      end
    end
  end

  describe '#close' do
    let(:client) do
      new_local_client_nmio([ DEFAULT_LOCAL_HOST ])
    end

    it 'disconnects the cluster and returns true' do
      RSpec::Mocks.with_temporary_scope do
        expect(client.cluster).to receive(:close).and_call_original
        expect(client.close).to be(true)
      end
    end
  end

  describe '#reconnect' do
    let(:client) do
      new_local_client_nmio([ ClusterConfig.instance.primary_address_str ])
    end

    it 'replaces the cluster' do
      old_id = client.cluster.object_id
      client.reconnect
      new_id = client.cluster.object_id
      expect(new_id).not_to eql(old_id)
    end

    it 'replaces the session pool' do
      old_id = client.cluster.session_pool.object_id
      client.reconnect
      new_id = client.cluster.session_pool.object_id
      expect(new_id).not_to eql(old_id)
    end

    it 'returns true' do
      expect(client.reconnect).to be(true)
    end
  end

  describe '#collections' do
    before do
      authorized_client.database[:users].drop
      authorized_client.database[:users].create
    end

    let(:collection) do
      Mongo::Collection.new(authorized_client.database, 'users')
    end

    it 'refers the current database collections' do
      expect(authorized_client.collections).to include(collection)
      expect(authorized_client.collections).to all(be_a(Mongo::Collection))
    end
  end

  describe '#start_session' do
    let(:session) do
      authorized_client.start_session
    end

    context 'when sessions are supported' do
      min_server_fcv '3.6'
      require_topology :replica_set, :sharded

      it 'creates a session' do
        expect(session).to be_a(Mongo::Session)
      end

      retry_test tries: 4
      it 'sets the last use field to the current time' do
        expect(session.instance_variable_get(:@server_session).last_use).to be_within(1).of(Time.now)
      end

      context 'when options are provided' do
        let(:options) do
          { causal_consistency: true }
        end

        let(:session) do
          authorized_client.start_session(options)
        end

        it 'sets the options on the session' do
          expect(session.options[:causal_consistency]).to eq(options[:causal_consistency])
        end
      end

      context 'when options are not provided' do
        it 'does not set options on the session' do
          expect(session.options).to eq({ implicit: false })
        end
      end

      context 'when a session is checked out and checked back in' do
        let!(:session_a) do
          authorized_client.start_session
        end

        let!(:session_b) do
          authorized_client.start_session
        end

        let!(:session_a_server_session) do
          session_a.instance_variable_get(:@server_session)
        end

        let!(:session_b_server_session) do
          session_b.instance_variable_get(:@server_session)
        end

        before do
          session_a_server_session.next_txn_num
          session_a_server_session.next_txn_num
          session_b_server_session.next_txn_num
          session_b_server_session.next_txn_num
          session_a.end_session
          session_b.end_session
        end

        it 'is returned to the front of the queue' do
          expect(authorized_client.start_session.instance_variable_get(:@server_session)).to be(session_b_server_session)
          expect(authorized_client.start_session.instance_variable_get(:@server_session)).to be(session_a_server_session)
        end

        it 'preserves the transaction numbers on the server sessions' do
          expect(authorized_client.start_session.next_txn_num).to be(3)
          expect(authorized_client.start_session.next_txn_num).to be(3)
        end
      end

      context 'when an implicit session is used' do
        before do
          authorized_client.database.command(ping: 1)
        end

        let(:pool) do
          authorized_client.cluster.session_pool
        end

        let!(:before_last_use) do
          pool.instance_variable_get(:@queue)[0].last_use
        end

        it 'uses the session and updates the last use time' do
          authorized_client.database.command(ping: 1)
          expect(before_last_use).to be < (pool.instance_variable_get(:@queue)[0].last_use)
        end
      end

      context 'when an implicit session is used without enough connections' do
        require_no_multi_mongos
        require_wired_tiger

        let(:client) do
          authorized_client.with(options).tap do |cl|
            cl.subscribe(Mongo::Monitoring::COMMAND, subscriber)
          end
        end

        let(:options) do
          { max_pool_size: 1, retry_writes: true }
        end

        shared_examples 'a single connection' do
          # JRuby, due to being concurrent, does not like rspec setting mocks
          # in threads while other threads are calling the methods being mocked.
          # My theory is that rspec removes & redefines methods as part of
          # the mocking process, but while a method is undefined JRuby is
          # running another thread that calls it leading to this exception:
          # NoMethodError: undefined method `with_connection' for #<Mongo::Server:0x5386 address=localhost:27017 PRIMARY>
          fails_on_jruby

          before do
            sessions_checked_out = 0

            allow_any_instance_of(Mongo::Server).to receive(:with_connection).and_wrap_original do |m, *args, **kwargs, &block|
              m.call(*args, **kwargs) do |connection|
                sessions_checked_out = 0
                res = block.call(connection)
                expect(sessions_checked_out).to be < 2
                res
              end
            end
          end

          it "doesn't have any live sessions" do
            threads.each do |thread|
              thread.join
            end
          end
        end

        context 'when doing three inserts' do
          let(:threads) do
            (1..3).map do |i|
              Thread.new do
                client['test'].insert_one({ test: "test#{i}" })
              end
            end
          end

          include_examples 'a single connection'
        end

        context 'when doing an insert and two updates' do
          let(:threads) do
            threads = []
            threads << Thread.new do
              client['test'].insert_one({ test: 'test' })
            end
            threads << Thread.new do
              client['test'].update_one({ test: 'test' }, { '$set' => { test: 'test2' } })
            end
            threads << Thread.new do
              client['test'].update_one({ test: 'test' }, { '$set' => { test: 'test2' } })
            end
            threads
          end

          include_examples 'a single connection'
        end

        context 'when doing an insert, update and delete' do
          let(:threads) do
            threads = []
            threads << Thread.new do
              client['test'].insert_one({ test: 'test' })
            end
            threads << Thread.new do
              client['test'].update_one({ test: 'test' }, { '$set' => { test: 'test2' } })
            end
            threads << Thread.new do
              client['test'].delete_one({ test: 'test' })
            end
            threads
          end

          include_examples 'a single connection'
        end

        context 'when doing an insert, update and find' do
          let(:threads) do
            threads = []
            threads << Thread.new do
              client['test'].insert_one({ test: 'test' })
            end
            threads << Thread.new do
              client['test'].update_one({ test: 'test' }, { '$set' => { test: 'test2' } })
            end
            threads << Thread.new do
              client['test'].find({ test: 'test' }).to_a
            end
            threads
          end

          include_examples 'a single connection'
        end

        context 'when doing an insert, update and bulk write' do
          let(:threads) do
            threads = []
            threads << Thread.new do
              client['test'].insert_one({ test: 'test' })
            end
            threads << Thread.new do
              client['test'].update_one({ test: 'test' }, { '$set' => { test: 'test2' } })
            end
            threads << Thread.new do
              client['test'].bulk_write([ { insert_one: { test: 'test1' } },
                                          { update_one: { filter: { test: 'test1' }, update: { '$set' => { test: 'test2' } } } } ])
            end
            threads
          end

          include_examples 'a single connection'
        end

        context 'when doing an insert, update and find_one_and_delete' do
          let(:threads) do
            threads = []
            threads << Thread.new do
              client['test'].insert_one({ test: 'test' })
            end
            threads << Thread.new do
              client['test'].update_one({ test: 'test' }, { '$set' => { test: 'test2' } })
            end
            threads << Thread.new do
              client['test'].find_one_and_delete({ test: 'test' })
            end
            threads
          end

          include_examples 'a single connection'
        end

        context 'when doing an insert, update and find_one_and_update' do
          let(:threads) do
            threads = []
            threads << Thread.new do
              client['test'].insert_one({ test: 'test' })
            end
            threads << Thread.new do
              client['test'].update_one({ test: 'test' }, { '$set' => { test: 'test2' } })
            end
            threads << Thread.new do
              client['test'].find_one_and_update({ test: 'test' }, { test: 'test2' })
            end
            threads
          end

          include_examples 'a single connection'
        end

        context 'when doing an insert, update and find_one_and_replace' do
          let(:threads) do
            threads = []
            threads << Thread.new do
              client['test'].insert_one({ test: 'test' })
            end
            threads << Thread.new do
              client['test'].update_one({ test: 'test' }, { '$set' => { test: 'test2' } })
            end
            threads << Thread.new do
              client['test'].find_one_and_replace({ test: 'test' }, { test: 'test2' })
            end
            threads
          end

          include_examples 'a single connection'
        end

        context 'when doing an insert, update and a replace' do
          let(:threads) do
            threads = []
            threads << Thread.new do
              client['test'].insert_one({ test: 'test' })
            end
            threads << Thread.new do
              client['test'].update_one({ test: 'test' }, { '$set' => { test: 'test2' } })
            end
            threads << Thread.new do
              client['test'].replace_one({ test: 'test' }, { test: 'test2' })
            end
            threads
          end

          include_examples 'a single connection'
        end

        context 'when doing all of the operations' do
          let(:threads) do
            threads = []
            threads << Thread.new do
              client['test'].insert_one({ test: 'test' })
            end
            threads << Thread.new do
              client['test'].update_one({ test: 'test' }, { '$set' => { test: 1 } })
            end
            threads << Thread.new do
              client['test'].find_one_and_replace({ test: 'test' }, { test: 'test2' })
            end
            threads << Thread.new do
              client['test'].delete_one({ test: 'test' })
            end
            threads << Thread.new do
              client['test'].find({ test: 'test' }).to_a
            end
            threads << Thread.new do
              client['test'].bulk_write([ { insert_one: { test: 'test1' } },
                                          { update_one: { filter: { test: 'test1' }, update: { '$set' => { test: 'test2' } } } } ])
            end
            threads << Thread.new do
              client['test'].find_one_and_delete({ test: 'test' })
            end
            threads << Thread.new do
              client['test'].find_one_and_update({ test: 'test' }, { test: 'test2' })
            end
            threads << Thread.new do
              client['test'].find_one_and_replace({ test: 'test' }, { test: 'test2' })
            end
            threads << Thread.new do
              client['test'].replace_one({ test: 'test' }, { test: 'test2' })
            end
            threads
          end

          include_examples 'a single connection'
        end
      end
    end

    context 'when two clients have the same cluster' do
      min_server_fcv '3.6'
      require_topology :replica_set, :sharded

      let(:client) do
        authorized_client.with(read: { mode: :secondary })
      end

      let(:session) do
        authorized_client.start_session
      end

      it 'allows the session to be used across the clients' do
        client[TEST_COLL].insert_one({ a: 1 }, session: session)
      end
    end

    context 'when two clients have different clusters' do
      min_server_fcv '3.6'
      require_topology :replica_set, :sharded

      let(:client) do
        another_authorized_client
      end

      let(:session) do
        authorized_client.start_session
      end

      it 'raises an exception' do
        expect {
          client[TEST_COLL].insert_one({ a: 1 }, session: session)
        }.to raise_exception(Mongo::Error::InvalidSession)
      end
    end

    context 'when sessions are not supported' do
      max_server_version '3.4'

      it 'raises an exception' do
        expect {
          session
        }.to raise_exception(Mongo::Error::InvalidSession)
      end
    end
  end

  describe '#summary' do
    context 'monitoring omitted' do
      let(:client) do
        new_local_client_nmio(
          [ DEFAULT_LOCAL_HOST ],
          read: { mode: :primary },
          database: SpecConfig.instance.test_db
        )
      end

      it 'indicates lack of monitoring' do
        expect(client.summary).to match /servers=.*UNKNOWN.*NO-MONITORING/
      end
    end

    context 'monitoring present' do
      require_topology :single, :replica_set, :sharded

      let(:client) do
        authorized_client
      end

      it 'does not indicate lack of monitoring' do
        expect(client.summary).to match /servers=.*(?:STANDALONE|PRIMARY|MONGOS)/
        expect(client.summary).not_to match /servers=.*(?:STANDALONE|PRIMARY|MONGOS).*NO-MONITORING/
      end
    end

    context 'background threads killed' do
      let(:client) do
        authorized_client.tap do |client|
          client.cluster.servers.map do |server|
            server.monitor&.stop!
          end
        end
      end

      it 'does not indicate lack of monitoring' do
        expect(client.summary).to match /servers=.*(STANDALONE|PRIMARY|MONGOS|\bLB\b).*NO-MONITORING/
      end
    end
  end
end
