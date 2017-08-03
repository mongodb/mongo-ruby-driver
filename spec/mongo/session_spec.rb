require 'spec_helper'

describe Mongo::Session, if: sessions_enabled? do

  let(:client) do
    authorized_client.with(client_options)
  end

  let(:session) do
    client.start_session(options)
  end

  after do
    session.end_session
  end

  let(:client_options) do
    { }
  end

  let(:options) do
    { }
  end

  describe '#client' do

    it 'returns the client through which the session was created' do
      expect(session.client).to be(client)
    end
  end

  describe '#options' do

    it 'returns the options' do
      expect(session.options).to be(options)
    end
  end

  describe '#initialize' do

    context 'when options are provided' do

      context 'when a read preference is provided' do

        let(:options) do
          { read: { mode: :primary } }
        end

        context 'when the client has a read preference' do

          let(:client_options) do
            { read: { mode: :secondary } }
          end

          it 'overrides the client read preference' do
            expect(session.read_preference).to eq(options[:read])
          end
        end

        context 'when the client does not have a read preference' do

          it 'saves the read preference' do
            expect(session.read_preference).to eq(options[:read])
          end
        end
      end

      context 'when a write concern is provided' do

        let(:options) do
          { write: { w: 2 } }
        end

        context 'when the client has a write concern' do

          let(:client_options) do
            { write: { w: 3 } }
          end

          it 'overrides the client write concern' do
            expect(session.write_concern.options).to eq(options[:write])
          end
        end

        context 'when the client does not have a write concern' do

          it 'saves the write concern' do
            expect(session.write_concern.options).to eq(options[:write])
          end
        end
      end
    end

    it 'considers the session active' do
      expect(session.ended?).to be(false)
    end

    it 'starts the session with a session id' do
      expect(session.ended?).to be(false)
    end

    it 'saves a reference to the client' do
      expect(session.client).to be(client)
    end

    it 'sets the operation time to nil' do
      expect(session.instance_variable_get(:@operation_time)).to be_nil
    end
  end

  describe '#end_session' do

    before do
      session.end_session
    end

    it 'sends an end session command to the server' do
      expect(session.instance_variable_get(:@server_session)).to receive(:end_sessions).and_call_original
      expect(session.ended?).to be(true)
    end

    it 'marks the session object as ended' do
      expect(session.ended?).to be(true)
    end

    it 'does not allow another database object to be accessed' do
      expect {
        session.database(TEST_DB)
      }.to raise_exception(Exception)
    end

    context 'when #end_session is called more than once' do

      before do
        session.end_session
        session.end_session
      end

      it 'considers the session ended' do
        expect(session.ended?).to be(true)
      end
    end
  end

  describe 'ended?' do

    context 'when the session is still active' do

      it 'returns false' do
        expect(session.ended?).to be(false)
      end
    end

    context 'when the session is ended' do

      before do
        session.end_session
      end

      it 'returns true' do
        expect(session.ended?).to be(true)
      end
    end
  end

  describe '#database' do

    let(:database) do
      session.database(TEST_DB)
    end

    context 'when the session is still active' do

      it 'returns a database' do
        expect(database).to be_a(Mongo::Database)
      end

      it 'returns a database with the name provided' do
        expect(database.name).to eq(TEST_DB)
      end

      it 'returns a database with options inherited from the client' do
        expect(database.options).to eq(client.options)
      end
    end

    context 'when the session has ended' do

      before do
        session.end_session
      end

      it 'raises and error' do
        expect {
          database
        }.to raise_exception(Exception)
      end
    end
  end

  describe '#with_recorded_operation_time' do

    context 'when the session is still active', if: replica_set? do

      let!(:operation_time_before) do
        session.database(TEST_DB)[TEST_COLL].find({}, limit: 1).first
        session.instance_variable_get(:@operation_time)
      end

      before do
        session.database(TEST_DB)[TEST_COLL].find({}, limit: 1).first
      end

      it 'records the operationTime' do
        expect(session.instance_variable_get(:@operation_time)).to be_a(BSON::Timestamp)
      end
    end

    context 'when the session has ended' do

      before do
        session.end_session
      end

      it 'raises and error' do
        expect {
          session.database(TEST_DB)[TEST_COLL].find({}, limit: 1).first
        }.to raise_exception(Exception)
      end
    end
  end

  describe '#database_names' do

    context 'when the session is still active' do

      it 'returns a list of database names' do
        expect(session.database_names).to include('admin')
      end
    end

    context 'when the session has ended' do

      before do
        session.end_session
      end

      it 'raises an exception' do
        expect {
          session.database_names
        }.to raise_exception(Exception)
      end
    end
  end

  describe '#list_databases' do

    context 'when the session is still active' do

      it 'returns a list of database info documents' do
        expect(
            session.list_databases.collect do |i|
              i['name']
            end).to include('admin')
      end
    end

    context 'when the session has ended' do

      before do
        session.end_session
      end

      it 'raises an exception' do
        expect {
          session.database_names
        }.to raise_exception(Exception)
      end
    end
  end

  context 'when the session does not use causally consistent reads' do

    let(:session) do
      authorized_client.start_session({})
    end

    let(:collection) do
      session.database(TEST_DB)[TEST_COLL, coll_options]
    end

    let(:client) do
      collection.client
    end

    let(:subscriber) do
      client.instance_variable_get(:@monitoring).subscribers[Mongo::Monitoring::COMMAND][-1]
    end

    around do |example|
      with_command_subscriber(client) do
        example.run
      end
    end

    let(:read_concern_document) do
      subscriber.instance_variable_get(:@started_events)['find'].command['readConcern']
    end

    context 'when the first request is sent' do

      context 'when the collection has a read concern set' do

        let(:coll_options) do
          { read_concern: { level: 'local' } }
        end

        before do
          collection.find({}, limit: 1).first
        end

        it 'does not add the afterClusterTime field' do
          expect(read_concern_document).to eq(BSON::Document.new(coll_options[:read_concern]))
        end
      end

      context 'when the collection does not have a read concern set' do

        let(:coll_options) do
          { }
        end

        before do
          collection.find({}, limit: 1).first
        end

        it 'does not send a read concern value' do
          expect(read_concern_document).to be_nil
        end
      end
    end

    context 'when the first request has already been sent' do

      context 'when the first request is successful' do

        context 'when the collection has a read concern set' do

          let(:coll_options) do
            { read_concern: { level: 'local' } }
          end

          let(:operation_time) do
            subscriber.instance_variable_get(:@succeeded_events)['find'].reply['operationTime']
          end

          let(:read_concern_document) do
            subscriber.instance_variable_get(:@started_events)['count'].command['readConcern']
          end

          before do
            collection.find({}, limit: 1).first
            collection.count
          end

          it 'does not add the operationTime as the afterClusterTime field' do
            expect(read_concern_document['afterClusterTime']).to be_nil
            expect(read_concern_document).to eq(coll_options[:read_concern])
          end
        end

        context 'when the collection does not have a read concern set' do

          let(:coll_options) do
            { }
          end

          let(:operation_time) do
            subscriber.instance_variable_get(:@succeeded_events)['find'].reply['operationTime']
          end

          let(:read_concern_document) do
            subscriber.instance_variable_get(:@started_events)['count'].command['readConcern']
          end

          before do
            collection.find({}, limit: 1).first
            collection.count
          end

          it 'does not add the operationTime as the afterClusterTime field' do
            expect(read_concern_document).to be_nil
            expect(read_concern_document).to be_nil
          end
        end
      end

      context 'when the first request is not successful' do

        context 'when the collection has a read concern set' do

          let(:coll_options) do
            { read_concern: { level: 'local' } }
          end

          let(:operation_time) do
            subscriber.instance_variable_get(:@succeeded_events)['find'].reply['operationTime']
          end

          let(:read_concern_document) do
            subscriber.instance_variable_get(:@started_events)['count'].command['readConcern']
          end

          before do
            begin; collection.find({ '$_id' => 1 }, limit: 1).first; rescue; end
            collection.count
          end

          it 'does not add the operationTime as the afterClusterTime field' do
            expect(read_concern_document['afterClusterTime']).to be_nil
            expect(read_concern_document).to eq(coll_options[:read_concern])
          end
        end

        context 'when the collection does not have a read concern set' do

          let(:coll_options) do
            { }
          end

          let(:operation_time) do
            subscriber.instance_variable_get(:@succeeded_events)['find'].reply['operationTime']
          end

          let(:read_concern_document) do
            subscriber.instance_variable_get(:@started_events)['count'].command['readConcern']
          end

          before do
            begin; collection.find({ '$_id' => 1 }, limit: 1).first; rescue; end
            collection.count
          end

          it 'does not add the operationTime as the afterClusterTime field' do
            expect(read_concern_document).to be_nil
            expect(read_concern_document).to be_nil
          end
        end
      end
    end
  end

  context 'when the session uses causally consistent reads' do

    let!(:session) do
      client.start_session(options)
    end

    let(:client) do
      authorized_client
    end

    let(:options) do
      { causally_consistent_reads: true }
    end

    let(:collection) do
      session.database(TEST_DB)[TEST_COLL, coll_options]
    end

    let(:coll_options) do
      { }
    end

    let!(:subscriber) do
      session.client.instance_variable_get(:@monitoring).subscribers[Mongo::Monitoring::COMMAND][-1]
    end

    around do |example|
      with_command_subscriber(session.client) do
        example.run
      end
    end

    context 'when the first request is sent' do

      let(:first_request_event) do
        subscriber.instance_variable_get(:@started_events)['find']
      end

      before do
        collection.find({}, limit: 1).first
      end

      context 'when the collection has a read concern set' do

        let(:coll_options) do
          { read_concern: { level: 'local' } }
        end

        it 'does not add the afterClusterTime field' do
          expect(first_request_event.command['readConcern']['afterClusterTime']).to be_nil
        end
      end

      context 'when the collection does not have a read concern set' do

        it 'does not include a readConcern value' do
          expect(first_request_event.command['readConcern']).to be_nil
        end
      end
    end

    context 'when at least one read request has been sent already', if: test_causally_consistent? do

      context 'when the first request is an OperationFailure' do

        let!(:first_request_event) do
          begin; collection.find({ '$a' => 1 }, limit: 1).first; rescue; end
          subscriber.instance_variable_get(:@started_events)['find']
        end

        let(:failed_operation_time) do
          subscriber.instance_variable_get(:@failed_events)['find'].reply['operationTime']
        end

        let(:second_request_event) do
          collection.count
          subscriber.instance_variable_get(:@started_events)['count']
        end

        it 'uses the operation time from the failed event in the subsequent request' do
          expect(failed_operation_time).to eq(second_request_event.command['readConcern']['afterClusterTime'])
        end

        it 'updates the operation time value on the session' do
          expect(session.instance_variable_get(:@operation_time)).to eq(failed_operation_time)
        end
      end

      context 'when the first request is successful' do

        let!(:first_request_event) do
          collection.find({ }, limit: 1).first
          subscriber.instance_variable_get(:@started_events)['find']
        end

        let(:second_request_event) do
          collection.count
          subscriber.instance_variable_get(:@started_events)['count']
        end

        let(:success_operation_time) do
          subscriber.instance_variable_get(:@succeeded_events)['find'].reply['operationTime']
        end

        let(:operation_time) do
          first_request_event['find'].reply['operationTime']
        end

        it 'updates the operation time value on the session' do
          expect(session.instance_variable_get(:@operation_time)).to eq(success_operation_time)
        end

        it 'adds the operationTime in the read concern document of the subsequent request' do
          expect(second_request_event.command['readConcern']['afterClusterTime']).to eq(success_operation_time)
        end
      end
    end

    context 'when at least one write request has been sent already', if: test_causally_consistent? do

      context 'when the first request is an OperationFailure' do

        let!(:first_request_event) do
          begin; collection.update_one({ '$a' => 1 }, {}).first; rescue; end
          subscriber.instance_variable_get(:@started_events)['update']
        end

        let(:failed_operation_time) do
          subscriber.instance_variable_get(:@succeeded_events)['update'].reply['operationTime']
        end

        let(:second_request_event) do
          collection.count
          subscriber.instance_variable_get(:@started_events)['count']
        end

        it 'updates the operation time value on the session' do
          expect(session.instance_variable_get(:@operation_time)).to eq(failed_operation_time)
        end

        it 'uses the operation time from the failed event in the subsequent request' do
          expect(second_request_event.command['readConcern']['afterClusterTime']).to eq(failed_operation_time)
        end
      end

      context 'when the first request is successful' do

        let!(:first_request_event) do
          begin; collection.update_one({ 'a' => 1 }, {}).first; rescue; end
          subscriber.instance_variable_get(:@started_events)['update']
        end

        let(:success_operation_time) do
          subscriber.instance_variable_get(:@succeeded_events)['update'].reply['operationTime']
        end

        let(:second_request_event) do
          collection.count
          subscriber.instance_variable_get(:@started_events)['count']
        end

        it 'updates the operation time value on the session' do
          expect(session.instance_variable_get(:@operation_time)).to eq(success_operation_time)
        end

        it 'adds the operationTime in the read concern document of the subsequent request' do
          expect(second_request_event.command['readConcern']['afterClusterTime']).to eq(success_operation_time)
        end
      end
    end
  end

  context 'when the session does not use causally consistent reads' do

    let(:session) do
      authorized_client.start_session(options)
    end

    let(:collection) do
      session.database(TEST_DB)[TEST_COLL, coll_options]
    end

    let(:coll_options) do
      { }
    end

    let(:client) do
      collection.client
    end

    let(:subscriber) do
      client.instance_variable_get(:@monitoring).subscribers[Mongo::Monitoring::COMMAND][-1]
    end

    around do |example|
      with_command_subscriber(client) do
        example.run
      end
    end

    let(:read_concern_document) do
      subscriber.instance_variable_get(:@started_events)['find'].command['readConcern']
    end

    context 'when the first request is successful' do

      before do
        2.times { collection.find({ 'a' => 1 }, limit: 1).first }
      end

      it 'does not include the afterClusterTime in the read concern document' do
        expect(read_concern_document).to be_nil
      end
    end
  end
end
