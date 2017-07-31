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
        expect(session.instance_variable_get(:@operation_time)).to be >(operation_time_before)
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
end
