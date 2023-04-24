# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Database do

  shared_context 'more than 100 collections' do
    let(:client) do
      root_authorized_client.use('many-collections')
    end

    before do
      120.times do |i|
        client["coll-#{i}"].drop
        client["coll-#{i}"].create
      end
    end
  end

  let(:subscriber) { Mrss::EventSubscriber.new }

  let(:monitored_client) do
    root_authorized_client.tap do |client|
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    end
  end

  describe '#==' do

    let(:database) do
      described_class.new(authorized_client, SpecConfig.instance.test_db)
    end

    context 'when the names are the same' do

      let(:other) do
        described_class.new(authorized_client, SpecConfig.instance.test_db)
      end

      it 'returns true' do
        expect(database).to eq(other)
      end
    end

    context 'when the names are not the same' do

      let(:other) do
        described_class.new(authorized_client, :other)
      end

      it 'returns false' do
        expect(database).to_not eq(other)
      end
    end

    context 'when the object is not a database' do

      it 'returns false' do
        expect(database).to_not eq('test')
      end
    end
  end

  describe '#[]' do

    let(:database) do
      described_class.new(authorized_client, SpecConfig.instance.test_db)
    end

    context 'when providing a valid name' do

      let(:collection) do
        database[:users]
      end

      it 'returns a new collection' do
        expect(collection.name).to eq('users')
      end
    end

    context 'when providing an invalid name' do

      it 'raises an error' do
        expect do
          database[nil]
        end.to raise_error(Mongo::Error::InvalidCollectionName)
      end
    end

    context 'when the client has options' do

      let(:client) do
        new_local_client([default_address.host], SpecConfig.instance.test_options.merge(read: { mode: :secondary }))
      end

      let(:database) do
        client.database
      end

      let(:collection) do
        database[:with_read_pref]
      end

      it 'applies the options to the collection' do
        expect(collection.server_selector).to eq(Mongo::ServerSelector.get(mode: :secondary))
        expect(collection.read_preference).to eq(BSON::Document.new(mode: :secondary))
      end

      context ':server_api option' do

        let(:client) do
          new_local_client_nmio(['localhost'], server_api: {version: '1'})
        end

        it 'is not transfered to the collection' do
          client.options[:server_api].should == {'version' => '1'}
          collection.options[:server_api].should be nil
        end
      end
    end

    context 'when providing :server_api option' do
      it 'is rejected' do
        lambda do
          database['foo', server_api: {version: '1'}]
        end.should raise_error(ArgumentError, 'The :server_api option cannot be specified for collection objects. It can only be specified on Client level')
      end
    end
  end

  describe '#collection_names' do

    let(:database) do
      described_class.new(authorized_client, SpecConfig.instance.test_db)
    end

    before do
      database['users'].drop
      database['users'].create
    end

    let(:actual) do
      database.collection_names
    end

    it 'returns the stripped names of the collections' do
      expect(actual).to include('users')
    end

    it 'does not include system collections' do
      expect(actual).to_not include('version')
      expect(actual).to_not include('system.version')
    end

    context 'on 2.6 server' do
      max_server_version '2.6'
    end

    it 'does not include collections with $ in names' do
      expect(actual.none? { |name| name.include?('$') }).to be true
    end

    context 'when provided a session' do

      let(:operation) do
        database.collection_names(session: session)
      end

      let(:client) do
        authorized_client
      end

      it_behaves_like 'an operation using a session'
    end

    context 'when specifying a batch size' do

      it 'returns the stripped names of the collections' do
        expect(database.collection_names(batch_size: 1).to_a).to include('users')
      end
    end

    context 'when there are more collections than the initial batch size' do

      before do
        2.times do |i|
          database["#{i}_dalmatians"].drop
        end
        2.times do |i|
          database["#{i}_dalmatians"].create
        end
      end

      it 'returns all collections' do
        collection_names = database.collection_names(batch_size: 1)
        expect(collection_names).to include('0_dalmatians')
        expect(collection_names).to include('1_dalmatians')
      end
    end

    context 'when provided a filter' do
      min_server_fcv '3.0'

      before do
        database['users2'].drop
        database['users2'].create
      end

      let(:result) do
        database.collection_names(filter: { name: 'users2' })
      end

      it 'returns users2 collection' do
        expect(result.length).to eq(1)
        expect(result.first).to eq('users2')
      end
    end

    context 'when provided authorized_collections or not' do

      context 'on server versions >= 4.0' do
        min_server_fcv '4.0'

        let(:database) do
          described_class.new(client, SpecConfig.instance.test_db)
        end

        let(:subscriber) { Mrss::EventSubscriber.new }

        let(:client) do
          authorized_client.tap do |client|
            client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
          end
        end

        context 'when authorized_collections is provided' do
          let(:options) do
            { authorized_collections: true }
          end

          let!(:result) do
            database.collections(options)
          end

          let(:events) do
            subscriber.command_started_events('listCollections')
          end

          it 'passes authorized_collections to the server' do
            expect(events.length).to eq(1)
            command = events.first.command
            expect(command['authorizedCollections']).to eq(true)
          end
        end

        context 'when no options are provided' do
          let!(:result) do
            database.collection_names
          end

          let(:events) do
            subscriber.command_started_events('listCollections')
          end

          it 'authorized_collections not passed to server' do
            expect(events.length).to eq(1)
            command = events.first.command
            expect(command['nameOnly']).to eq(true)
            expect(command['authorizedCollections']).to be_nil
          end
        end
      end
    end

    context 'when there are more than 100 collections' do
      include_context 'more than 100 collections'

      let(:collection_names) do
        client.database.collection_names.sort
      end

      it 'lists all collections' do
        collection_names.length.should == 120
        collection_names.should include('coll-0')
        collection_names.should include('coll-119')
      end
    end

    context 'with comment' do
      min_server_version '4.4'

      it 'returns collection names and send comment' do
        database = described_class.new(monitored_client, SpecConfig.instance.test_db)
        database.collection_names(comment: "comment")
        command = subscriber.command_started_events("listCollections").last&.command
        expect(command).not_to be_nil
        expect(command["comment"]).to eq("comment")
      end
    end
  end

  describe '#list_collections' do

    let(:database) do
      described_class.new(authorized_client, SpecConfig.instance.test_db)
    end

    let(:result) do
      database.list_collections.map do |info|
        info['name']
      end
    end

    before do
      database['acol'].drop
      database['acol'].create
    end

    context 'server 3.0+' do
      min_server_fcv '3.0'

      it 'returns a list of the collections info' do
        expect(result).to include('acol')
      end

      context 'with more than one collection' do
        before do
          database['anothercol'].drop
          database['anothercol'].create

          expect(database.collections.length).to be > 1
        end

        let(:result) do
          database.list_collections(filter: { name: 'anothercol' }).map do |info|
            info['name']
          end
        end

        it 'can filter by collection name' do
          expect(result.length).to eq(1)
          expect(result.first).to eq('anothercol')
        end
      end
    end

    context 'server 2.6' do
      max_server_fcv '2.6'

      it 'returns a list of the collections info' do
        expect(result).to include("#{SpecConfig.instance.test_db}.acol")
      end
    end

    it 'does not include collections with $ in names' do
      expect(result.none? { |name| name.include?('$') }).to be true
    end

    context 'on admin database' do
      let(:database) do
        described_class.new(root_authorized_client, 'admin')
      end

      shared_examples 'does not include system collections' do
        it 'does not include system collections' do
          expect(result.none? { |name| name =~ /(^|\.)system\./ }).to be true
        end
      end

      context 'server 4.7+' do
        min_server_fcv '4.7'
        # https://jira.mongodb.org/browse/SERVER-35804
        require_topology :single, :replica_set

        include_examples 'does not include system collections'

        it 'returns results' do
          expect(result).to include('acol')
        end
      end

      context 'server 3.0-4.5' do
        min_server_fcv '3.0'
        max_server_version '4.5'

        include_examples 'does not include system collections'

        it 'returns results' do
          expect(result).to include('acol')
        end
      end

      context 'server 2.6' do
        max_server_version '2.6'

        include_examples 'does not include system collections'

        it 'returns results' do
          expect(result).to include('admin.acol')
        end
      end
    end

    context 'when provided authorized_collections or name_only options or not' do

      context 'on server versions >= 4.0' do
        min_server_fcv '4.0'

        let(:database) do
          described_class.new(client, SpecConfig.instance.test_db)
        end

        let(:subscriber) { Mrss::EventSubscriber.new }

        let(:client) do
          authorized_client.tap do |client|
            client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
          end
        end

        context 'when both are provided' do
          let(:options) do
            { name_only: true, authorized_collections: true }
          end

          let!(:result) do
            database.list_collections(options)
          end

          let(:events) do
            subscriber.command_started_events('listCollections')
          end

          it 'passes original options to the server' do
            expect(events.length).to eq(1)
            command = events.first.command
            expect(command['nameOnly']).to eq(true)
            expect(command['authorizedCollections']).to eq(true)
          end
        end

        context 'when name_only is provided' do
          let(:options) do
            { name_only: false }
          end

          let!(:result) do
            database.list_collections(options)
          end

          let(:events) do
            subscriber.command_started_events('listCollections')
          end

          it 'no options passed to server because false' do
            expect(events.length).to eq(1)
            command = events.first.command
            expect(command['nameOnly']).to be_nil
            expect(command['authorizedCollections']).to be_nil
          end
        end

        context 'when no options provided' do

          let!(:result) do
            database.list_collections
          end

          let(:events) do
            subscriber.command_started_events('listCollections')
          end

          it 'no options passed to server because none provided' do
            expect(events.length).to eq(1)
            command = events.first.command
            expect(command['nameOnly']).to be_nil
            expect(command['authorizedCollections']).to be_nil
          end
        end
      end
    end

    context 'when there are more than 100 collections' do
      include_context 'more than 100 collections'

      let(:collections) do
        client.database.list_collections
      end

      let(:collection_names) do
        # 2.6 server prefixes collection names with database name
        collections.map { |info| info['name'].sub(/^many-collections\./, '') }.sort
      end

      it 'lists all collections' do
        collections.length.should == 120
        collection_names.should include('coll-0')
        collection_names.should include('coll-119')
      end
    end

    context 'with comment' do
      min_server_version '4.4'

      it 'returns collection names and send comment' do
        database = described_class.new(monitored_client, SpecConfig.instance.test_db)
        database.list_collections(comment: "comment")
        command = subscriber.command_started_events("listCollections").last&.command
        expect(command).not_to be_nil
        expect(command["comment"]).to eq("comment")
      end
    end
  end

  describe '#collections' do

    context 'when the database exists' do

      let(:database) do
        described_class.new(authorized_client, SpecConfig.instance.test_db)
      end

      let(:collection) do
        Mongo::Collection.new(database, 'users')
      end

      before do
        database['users'].drop
        database['users'].create
      end

      it 'returns collection objects for each name' do
        expect(database.collections).to include(collection)
      end

      it 'does not include collections with $ in names' do
        expect(database.collections.none? { |c| c.name.include?('$') }).to be true
      end
    end

    context 'on admin database' do

      let(:database) do
        described_class.new(root_authorized_client, 'admin')
      end

      it 'does not include the system collections' do
        collection_names = database.collections.map(&:name)
        expect(collection_names).not_to include('system.version')
        expect(collection_names.none? { |name| name =~ /(^|\.)system\./ }).to be true
      end
    end

    context 'when the database does not exist' do

      let(:database) do
        described_class.new(authorized_client, 'invalid_database')
      end

      it 'returns an empty list' do
        expect(database.collections).to be_empty
      end
    end

    context 'when the user is not authorized' do
      require_auth

      let(:database) do
        described_class.new(unauthorized_client, SpecConfig.instance.test_db)
      end

      it 'raises an exception' do
        expect {
          database.collections
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end

    context 'when provided a filter' do
      min_server_fcv '3.0'

      let(:database) do
        described_class.new(authorized_client, SpecConfig.instance.test_db)
      end

      let(:collection2) do
        Mongo::Collection.new(database, 'users2')
      end

      before do
        database['users1'].drop
        database['users1'].create

        database['users2'].drop
        database['users2'].create
      end

      let(:result) do
        database.collections(filter: { name: 'users2' })
      end

      it 'returns users2 collection' do
        expect(result.length).to eq(1)
        expect(database.collections).to include(collection2)
      end
    end

    context 'when provided authorized_collections or not' do

      context 'on server versions >= 4.0' do
        min_server_fcv '4.0'

        let(:database) do
          described_class.new(client, SpecConfig.instance.test_db)
        end

        let(:subscriber) { Mrss::EventSubscriber.new }

        let(:client) do
          authorized_client.tap do |client|
            client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
          end
        end

        context 'when authorized_collections are provided as false' do
          let(:options) do
            { authorized_collections: false }
          end

          let!(:result) do
            database.collections(options)
          end

          let(:events) do
            subscriber.command_started_events('listCollections')
          end

          it 'authorized_collections not passed to server because false' do
            expect(events.length).to eq(1)
            command = events.first.command
            expect(command['nameOnly']).to eq(true)
            expect(command['authorizedCollections']).to be_nil
          end
        end

        context 'when authorized_collections are provided as true' do
          let(:options) do
            { authorized_collections: true }
          end

          let!(:result) do
            database.collections(options)
          end

          let(:events) do
            subscriber.command_started_events('listCollections')
          end

          it 'authorized_collections not passed to server because false' do
            expect(events.length).to eq(1)
            command = events.first.command
            expect(command['nameOnly']).to eq(true)
            expect(command['authorizedCollections']).to eq(true)
          end
        end

        context 'when no options are provided' do
          let!(:result) do
            database.collections
          end

          let(:events) do
            subscriber.command_started_events('listCollections')
          end

          it 'authorized_collections not passed to server because not provided' do
            expect(events.length).to eq(1)
            command = events.first.command
            expect(command['authorizedCollections']).to be_nil
          end
        end
      end
    end

    context 'when there are more than 100 collections' do
      include_context 'more than 100 collections'

      let(:collections) do
        client.database.collections
      end

      let(:collection_names) do
        collections.map(&:name).sort
      end

      it 'lists all collections' do
        collections.length.should == 120
        collection_names.should include('coll-0')
        collection_names.should include('coll-119')
      end
    end

    context 'with comment' do
      min_server_version '4.4'

      it 'returns collection names and send comment' do
        database = described_class.new(monitored_client, SpecConfig.instance.test_db)
        database.collections(comment: "comment")
        command = subscriber.command_started_events("listCollections").last&.command
        expect(command).not_to be_nil
        expect(command["comment"]).to eq("comment")
      end
    end
  end

  describe '#command' do

    let(:database) do
      described_class.new(authorized_client, SpecConfig.instance.test_db)
    end

    it 'sends the query command to the cluster' do
      expect(database.command(:ping => 1).written_count).to eq(0)
    end

    it 'does not mutate the command selector' do
      expect(database.command({:ping => 1}.freeze).written_count).to eq(0)
    end

    context 'when provided a session' do
      min_server_fcv '3.6'

      let(:operation) do
        client.database.command({ :ping => 1 }, session: session)
      end

      let(:failed_operation) do
        client.database.command({ :invalid => 1 }, session: session)
      end

      let(:session) do
        client.start_session
      end

      let(:subscriber) { Mrss::EventSubscriber.new }

      let(:client) do
        authorized_client.tap do |client|
          client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
        end
      end

      it_behaves_like 'an operation using a session'
      it_behaves_like 'a failed operation using a session'


      let(:full_command) do
        subscriber.started_events.find { |cmd| cmd.command_name == 'ping' }.command
      end

      it 'does not add a afterClusterTime field' do
        # Ensure that the session has an operation time
        client.database.command({ ping: 1 }, session: session)
        operation
        expect(full_command['readConcern']).to be_nil
      end
    end

    context 'when a read concern is provided' do
      min_server_fcv '3.2'

      context 'when the read concern is valid' do

        it 'sends the read concern' do
          expect {
            database.command(:ping => 1, readConcern: { level: 'local' })
          }.to_not raise_error
        end
      end

      context 'when the read concern is not valid' do
        require_topology :single, :replica_set

        it 'raises an exception' do
          expect {
            database.command(:ping => 1, readConcern: { level: 'yay' })
          }.to raise_error(Mongo::Error::OperationFailure)
        end
      end
    end

    context 'when no read preference is provided' do
      require_topology :single, :replica_set

      let!(:primary_server) do
        database.cluster.next_primary
      end

      it 'uses read preference of primary' do
        RSpec::Mocks.with_temporary_scope do
          expect(primary_server).to receive(:with_connection).with(any_args).and_call_original

          expect(database.command(ping: 1)).to be_successful
        end
      end
    end

    context 'when the client has a read preference set' do
      require_topology :single, :replica_set

      let!(:primary_server) do
        database.cluster.next_primary
      end

      let(:read_preference) do
        { :mode => :secondary, :tag_sets => [{ 'non' => 'existent' }] }
      end

      let(:client) do
        authorized_client.with(read: read_preference)
      end

      let(:database) do
        described_class.new(client, SpecConfig.instance.test_db, client.options)
      end

      it 'does not use the client read preference 'do
        RSpec::Mocks.with_temporary_scope do
          expect(primary_server).to receive(:with_connection).with(any_args).and_call_original

          expect(database.command(ping: 1)).to be_successful
        end
      end
    end

    context 'when there is a read preference argument provided' do
      require_topology :single, :replica_set

      let(:read_preference) do
        { :mode => :secondary, :tag_sets => [{ 'non' => 'existent' }] }
      end

      let(:client) do
        authorized_client.with(server_selection_timeout: 0.2)
      end

      let(:database) do
        described_class.new(client, SpecConfig.instance.test_db, client.options)
      end

      before do
        allow(database.cluster).to receive(:single?).and_return(false)
      end

      it 'uses the read preference argument' do
        expect {
          database.command({ ping: 1 }, read: read_preference)
        }.to raise_error(Mongo::Error::NoServerAvailable)
      end
    end

    context 'when the client has a server_selection_timeout set' do
      require_topology :single, :replica_set

      let(:client) do
        authorized_client.with(server_selection_timeout: 0)
      end

      let(:database) do
        described_class.new(client, SpecConfig.instance.test_db, client.options)
      end

      it 'uses the client server_selection_timeout' do
        expect {
          database.command(ping: 1)
        }.to raise_error(Mongo::Error::NoServerAvailable)
      end
    end

    context 'when a write concern is not defined on the client/database object' do

      context 'when a write concern is provided in the selector' do
        require_topology :single

        let(:cmd) do
          {
              insert: TEST_COLL,
              documents: [ { a: 1 } ],
              writeConcern: INVALID_WRITE_CONCERN
          }
        end

        it 'uses the write concern' do
          expect {
            database.command(cmd)
          }.to raise_exception(Mongo::Error::OperationFailure)
        end
      end
    end

    context 'when a write concern is defined on the client/database object' do

      let(:client_options) do
        {
          write: INVALID_WRITE_CONCERN
        }
      end

      let(:database) do
        described_class.new(authorized_client.with(client_options), SpecConfig.instance.test_db)
      end

      context 'when a write concern is not in the command selector' do

        let(:cmd) do
          {
              insert: TEST_COLL,
              documents: [ { a: 1 } ]
          }
        end

        it 'does not apply a write concern' do
          expect(database.command(cmd).written_count).to eq(1)
        end
      end

      context 'when a write concern is provided in the command selector' do
        require_topology :single

        let(:cmd) do
          {
              insert: TEST_COLL,
              documents: [ { a: 1 } ],
              writeConcern: INVALID_WRITE_CONCERN
          }
        end

        it 'uses the write concern' do
          expect {
            database.command(cmd)
          }.to raise_exception(Mongo::Error::OperationFailure)
        end
      end
    end

    context 'when client server api is not set' do
      require_no_required_api_version
      min_server_fcv '4.7'

      it 'passes server api parameters' do
        lambda do
          database.command(ping: 1, apiVersion: 'does-not-exist')
        end.should raise_error(
          an_instance_of(Mongo::Error::OperationFailure).and having_attributes(code: 322))
      end
    end

    context 'when client server api is set' do
      require_required_api_version
      min_server_fcv '4.7'

      it 'reports server api conflict' do
        lambda do
          database.command(ping: 1, apiVersion: 'does-not-exist')
        end.should raise_error(Mongo::Error::ServerApiConflict)
      end
    end
  end

  describe '#drop' do

    let(:database) do
      described_class.new(authorized_client, SpecConfig.instance.test_db)
    end

    it 'drops the database' do
      expect(database.drop).to be_successful
    end

    context 'when provided a session' do

      let(:operation) do
        database.drop(session: session)
      end

      let(:client) do
        authorized_client
      end

      it_behaves_like 'an operation using a session'
    end

    context 'when the client/database has a write concern' do

      let(:client_options) do
        {
          write: INVALID_WRITE_CONCERN,
          database: :safe_to_drop
        }
      end

      let(:client) do
        root_authorized_client.with(client_options)
      end

      let(:database_with_write_options) do
        client.database
      end

      context 'when the server supports write concern on the dropDatabase command' do
        min_server_fcv '3.4'
        require_topology :single

        it 'applies the write concern' do
          expect{
            database_with_write_options.drop
          }.to raise_exception(Mongo::Error::OperationFailure)
        end
      end

      context 'when write concern is passed in as an option' do
        min_server_fcv '3.4'
        require_topology :single

        let(:client_options) do
          {
            write_concern: {w: 0},
            database: :test
          }
        end

        let(:session) do
          client.start_session
        end

        let(:subscriber) { Mrss::EventSubscriber.new }

        let(:client) do
          root_authorized_client.tap do |client|
            client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
          end.with(client_options)
        end

        let(:events) do
          subscriber.command_started_events('dropDatabase')
        end

        let(:database_test_wc) do
          client.database
        end

        let!(:command) do
          Utils.get_command_event(client, 'dropDatabase') do |client|
            database_test_wc.drop({ write_concern: {w: 'majority'} })
          end.command
        end

        it 'applies the write concern passed in as an option' do
          expect(events.length).to eq(1)
          expect(command).to_not be_nil
          expect(command[:writeConcern][:w]).to eq('majority')
        end
      end

      context 'when the server does not support write concern on the dropDatabase command' do
        max_server_version '3.2'

        it 'does not apply the write concern' do
          expect(database_with_write_options.drop).to be_successful
        end
      end
    end
  end

  describe '#initialize' do

    context 'when provided a valid name' do

      let(:database) do
        described_class.new(authorized_client, SpecConfig.instance.test_db)
      end

      it 'sets the name as a string' do
        expect(database.name).to eq(SpecConfig.instance.test_db)
      end

      it 'sets the client' do
        expect(database.client).to eq(authorized_client)
      end
    end

    context 'when the name is nil' do

      it 'raises an error' do
        expect do
          described_class.new(authorized_client, nil)
        end.to raise_error(Mongo::Error::InvalidDatabaseName)
      end
    end
  end

  describe '#inspect' do

    let(:database) do
      described_class.new(authorized_client, SpecConfig.instance.test_db)
    end

    it 'includes the object id' do
      expect(database.inspect).to include(database.object_id.to_s)
    end

    it 'includes the name' do
      expect(database.inspect).to include(database.name)
    end
  end

  describe '#fs' do
    require_topology :single, :replica_set

    let(:database) do
      described_class.new(authorized_client, SpecConfig.instance.test_db)
    end

    shared_context 'a GridFS database' do

      it 'returns a Grid::FS for the db' do
        expect(fs).to be_a(Mongo::Grid::FSBucket)
      end

      context 'when operating on the fs' do

        let(:file) do
          Mongo::Grid::File.new('Hello!', :filename => 'test.txt')
        end

        before do
          fs.files_collection.delete_many
          fs.chunks_collection.delete_many
        end

        let(:from_db) do
          fs.insert_one(file)
          fs.find({ filename: 'test.txt' }, limit: 1).first
        end

        it 'returns the assembled file from the db' do
          expect(from_db['filename']).to eq(file.info.filename)
        end
      end
    end

    context  'when no options are provided' do

      let(:fs) do
        database.fs
      end

      it_behaves_like 'a GridFS database'
    end

    context 'when a custom prefix is provided' do

      context 'when the option is fs_name' do

        let(:fs) do
          database.fs(:fs_name => 'grid')
        end

        it 'sets the custom prefix' do
          expect(fs.prefix).to eq('grid')
        end

        it_behaves_like 'a GridFS database'
      end

      context 'when the option is bucket_name' do

        let(:fs) do
          database.fs(:bucket_name => 'grid')
        end

        it 'sets the custom prefix' do
          expect(fs.prefix).to eq('grid')
        end

        it_behaves_like 'a GridFS database'
      end
    end
  end

  describe '#write_concern' do

    let(:client) do
      new_local_client(['127.0.0.1:27017'],
        {monitoring_io: false}.merge(client_options))
    end

    let(:database) { client.database }

    context 'when client write concern uses :write' do

      let(:client_options) do
        { :write => { :w => 1 } }
      end

      it 'is the correct write concern' do
        expect(database.write_concern).to be_a(Mongo::WriteConcern::Acknowledged)
        expect(database.write_concern.options).to eq(w: 1)
      end
    end

    context 'when client write concern uses :write_concern' do

      let(:client_options) do
        { :write_concern => { :w => 1 } }
      end

      it 'is the correct write concern' do
        expect(database.write_concern).to be_a(Mongo::WriteConcern::Acknowledged)
        expect(database.write_concern.options).to eq(w: 1)
      end
    end
  end

  describe '#aggregate' do
    min_server_fcv '3.6'

    let(:client) do
      root_authorized_admin_client
    end

    let(:database) { client.database }

    let(:pipeline) do
      [{'$currentOp' => {}}]
    end

    describe 'updating cluster time' do
      # The shared examples use their own client which we cannot override
      # from here, and it uses the wrong credentials for admin database which
      # is the one we need for our pipeline when auth is on.
      require_no_auth

      let(:database_via_client) do
        client.use(:admin).database
      end

      let(:operation) do
        database_via_client.aggregate(pipeline).first
      end

      let(:operation_with_session) do
        database_via_client.aggregate(pipeline, session: session).first
      end

      let(:second_operation) do
        database_via_client.aggregate(pipeline, session: session).first
      end

      it_behaves_like 'an operation updating cluster time'
    end

    it 'returns an Aggregation object' do
      expect(database.aggregate(pipeline)).to be_a(Mongo::Collection::View::Aggregation)
    end

    context 'when options are provided' do

      let(:options) do
        { :allow_disk_use => true, :bypass_document_validation => true }
      end

      it 'sets the options on the Aggregation object' do
        expect(database.aggregate(pipeline, options).options).to eq(BSON::Document.new(options))
      end

      context 'when the :comment option is provided' do

        let(:options) do
          { :comment => 'testing' }
        end

        it 'sets the options on the Aggregation object' do
          expect(database.aggregate(pipeline, options).options).to eq(BSON::Document.new(options))
        end
      end

      context 'when a session is provided' do

        let(:session) do
          client.start_session
        end

        let(:operation) do
          database.aggregate(pipeline, session: session).to_a
        end

        let(:failed_operation) do
          database.aggregate([ { '$invalid' => 1 }], session: session).to_a
        end

        it_behaves_like 'an operation using a session'
        it_behaves_like 'a failed operation using a session'
      end

      context 'when a hint is provided' do

        let(:options) do
          { 'hint' => { 'y' => 1 } }
        end

        it 'sets the options on the Aggregation object' do
          expect(database.aggregate(pipeline, options).options).to eq(options)
        end
      end

      context 'when collation is provided' do

        let(:pipeline) do
          [{ "$currentOp" => {} }]
        end

        let(:options) do
          { collation: { locale: 'en_US', strength: 2 } }
        end

        let(:result) do
          database.aggregate(pipeline, options).collect { |doc| doc.keys.grep(/host/).first }
        end

        context 'when the server selected supports collations' do
          min_server_fcv '3.4'

          it 'applies the collation' do
            expect(result.uniq).to eq(['host'])
          end
        end

        context 'when the server selected does not support collations' do
          max_server_version '3.2'

          it 'raises an exception' do
            expect {
              result
            }.to raise_exception(Mongo::Error::UnsupportedCollation)
          end

          context 'when a String key is used' do

            let(:options) do
              { 'collation' => { locale: 'en_US', strength: 2 } }
            end

            it 'raises an exception' do
              expect {
                result
              }.to raise_exception(Mongo::Error::UnsupportedCollation)
            end
          end
        end
      end
    end
  end
end
