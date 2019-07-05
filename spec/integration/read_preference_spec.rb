require 'spec_helper'

# The only allowed read preference in transaction is primary.
# Because of this, the tests assert that the final read preference is primary.
# It would be preferable to assert that some other read preference is selected,
# but this would only work for non-transactional tests and would require
# duplicating the examples.

describe 'Read preference' do
  require_topology :replica_set
  clean_slate_on_evergreen

  let(:client) do
    authorized_client.with(client_options)
  end

  let(:subscriber) { EventSubscriber.new }

  before do
    client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
  end

  let(:client_options) do
    {}
  end

  let(:session_options) do
    {}
  end

  let(:tx_options) do
    {}
  end

  let(:collection) { client['tx_read_pref_test'] }

  before do
    collection.drop
    collection.create(write_concern: {w: :majority})
  end

  let(:find_options) do
    {}
  end

  shared_examples_for 'sends expected read preference when reading' do
    it 'sends expected read preference when reading' do
      read_operation

      event = subscriber.single_command_started_event('find')
      actual_preference = event.command['$readPreference']
      expect(actual_preference).to eq(expected_read_preference)
    end
  end

  shared_examples_for 'does not send read preference when reading' do
    it 'does not send read preference when reading' do
      read_operation

      event = subscriber.single_command_started_event('find')
      actual_preference = event.command['$readPreference']
      expect(actual_preference).to be nil
    end
  end

  shared_examples_for 'non-transactional read preference examples' do
    it 'does not send read preference when writing' do
      write_operation

      event = subscriber.single_command_started_event('insert')
      actual_preference = event.command['$readPreference']
      expect(actual_preference).to be nil
    end

    it_behaves_like 'sends expected read preference when reading'
  end

  shared_examples_for 'sends expected read preference' do
    it_behaves_like 'non-transactional read preference examples'
  end

  shared_context 'non-transactional read preference specifications' do

    context 'when read preference is not explicitly given' do
      let(:client_options) do
        {}
      end

      let(:expected_read_preference) do
        nil
      end

      it_behaves_like 'sends expected read preference'
    end

    context 'when read preference is given in client options' do
      let(:client_options) do
        {read: { mode: :primary }}
      end

      let(:expected_read_preference) do
        {'mode' => 'primary'}
      end

      it_behaves_like 'sends expected read preference'
    end

    context 'when read preference is given in operation options' do
      let(:expected_read_preference) do
        {'mode' => 'primary'}
      end

      let(:find_options) do
        {read: {mode: :primary}}
      end

      it_behaves_like 'sends expected read preference'
    end

    context 'when read preference is given in client and operation options' do
      let(:client_options) do
        {read: { mode: :secondary }}
      end

      # Operation should override the client.
      let(:expected_read_preference) do
        {'mode' => 'primary'}
      end

      let(:find_options) do
        {read: {mode: :primary}}
      end

      it_behaves_like 'sends expected read preference'
    end

    context 'when read preference is given in collection and operation options' do
      let(:collection) do
        client['tx_read_pref_test', {read: {mode: :secondary}}]
      end

      # Operation should override the collection.
      let(:expected_read_preference) do
        {'mode' => 'primary'}
      end

      let(:find_options) do
        {read: {mode: :primary}}
      end

      it_behaves_like 'sends expected read preference'
    end
  end

  context 'not in transaction' do

    let(:write_operation) do
      collection.insert_one(hello: 'world')
    end

    let(:read_operation) do
      collection.with(write: {w: :majority}).insert_one(hello: 'world')
      res = collection.find({}, find_options || {}).to_a.count
      expect(res).to eq(1)
    end

    include_context 'non-transactional read preference specifications'

    context 'when read preference is given in collection options' do
      let(:client_options) do
        {}
      end

      let(:collection) do
        client['tx_read_pref_test', {read: {mode: :primary}}]
      end

      let(:expected_read_preference) do
        {'mode' => 'primary'}
      end

      it_behaves_like 'sends expected read preference'
    end

    context 'when read preference is given in collection options via #with' do
      let(:collection) do
        client['tx_read_pref_test'].with(read: {mode: :primary})
      end

      let(:expected_read_preference) do
        {'mode' => 'primary'}
      end

      it_behaves_like 'sends expected read preference'
    end

    context 'when read preference is given in client and collection options' do
      let(:client_options) do
        {read: { mode: :secondary }}
      end

      let(:collection) do
        client['tx_read_pref_test', {read: {mode: :primary}}]
      end

      # Collection should override the client.
      let(:expected_read_preference) do
        {'mode' => 'primary'}
      end

      it_behaves_like 'sends expected read preference'
    end
  end

  context 'in transaction' do
    min_server_fcv '4.0'

    let(:write_operation) do
      expect do
        session = client.start_session(session_options)
        session.with_transaction(tx_options) do
          collection.insert_one({hello: 'world'}, session: session)
        end
      end.not_to raise_error
    end

    let(:read_operation) do
      expect do
        session = client.start_session(session_options)
        session.with_transaction(tx_options) do
          collection.insert_one({hello: 'world'}, session: session)
          res = collection.find({}, {session: session}.merge(find_options || {})).to_a.count
          expect(res).to eq(1)
        end
      end.not_to raise_error
    end

    shared_examples_for 'sends expected read preference' do
      it_behaves_like 'non-transactional read preference examples'

      it 'sends expected read preference when starting transaction' do
        collection.insert_one(hello: 'world')

        session = client.start_session(session_options)
        session.with_transaction(tx_options) do
          res = collection.find({}, {session: session}.merge(find_options || {})).to_a.count
          expect(res).to eq(1)
        end

        event = subscriber.single_command_started_event('find')
        actual_preference = event.command['$readPreference']
        expect(actual_preference).to eq(expected_read_preference)
      end
    end

    include_context 'non-transactional read preference specifications'

    context 'when read preference is given in collection options' do
      let(:client_options) do
        {}
      end

      let(:collection) do
        client['tx_read_pref_test', {read: {mode: :primary}}]
      end

      # collection read preference is ignored
      let(:expected_read_preference) do
        nil
      end

      it_behaves_like 'sends expected read preference'
    end

    context 'when read preference is given in collection options via #with' do
      let(:collection) do
        client['tx_read_pref_test'].with(read: {mode: :primary})
      end

      # collection read preference is ignored
      let(:expected_read_preference) do
        nil
      end

      it_behaves_like 'sends expected read preference'
    end

    context 'when read preference is given in client and collection options' do
      let(:client_options) do
        {read: { mode: :primary }}
      end

      let(:collection) do
        client['tx_read_pref_test', {read: {mode: :secondary}}]
      end

      # collection read preference is ignored, client read preference is used
      let(:expected_read_preference) do
        {'mode' => 'primary'}
      end

      it_behaves_like 'sends expected read preference'
    end

    context 'when read preference is given in default transaction options' do
      let(:session_options) do
        {default_transaction_options: {read: { mode: :primary }}}
      end

      let(:expected_read_preference) do
        {'mode' => 'primary'}
      end

      it_behaves_like 'sends expected read preference'
    end

    context 'when read preference is given in client and default transaction options' do
      let(:client_options) do
        {read: { mode: :secondary }}
      end

      let(:session_options) do
        {default_transaction_options: {read: { mode: :primary }}}
      end

      let(:expected_read_preference) do
        {'mode' => 'primary'}
      end

      it_behaves_like 'sends expected read preference'
    end

    context 'when read preference is given in collection and default transaction options' do
      let(:collection) do
        client['tx_read_pref_test', {read: {mode: :secondary}}]
      end

      let(:session_options) do
        {default_transaction_options: {read: { mode: :primary }}}
      end

      let(:expected_read_preference) do
        {'mode' => 'primary'}
      end

      it_behaves_like 'sends expected read preference'
    end

    context 'when read preference is given in default transaction and transaction options' do
      let(:session_options) do
        {default_transaction_options: {read: { mode: :secondary }}}
      end

      let(:tx_options) do
        {read: { mode: :primary }}
      end

      let(:expected_read_preference) do
        {'mode' => 'primary'}
      end

      it_behaves_like 'sends expected read preference'
    end

    context 'when read preference is given in default transaction and operation options' do
      let(:session_options) do
        {default_transaction_options: {read: { mode: :primary }}}
      end

      let(:find_options) do
        {read: {mode: :secondary}}
      end

      let(:expected_read_preference) do
        {'mode' => 'primary'}
      end

      it 'sends operation read preference and fails' do
        expect do
          session = client.start_session(session_options)
          session.with_transaction(tx_options) do
            collection.insert_one({hello: 'world'}, session: session)
            res = collection.find({}, {session: session}.merge(find_options || {})).to_a.count
            expect(res).to eq(1)
          end
        end.to raise_error(Mongo::Error::InvalidTransactionOperation, "read preference in a transaction must be primary (requested: secondary)")
      end
    end

    context 'when read preference is given in transaction options' do
      let(:tx_options) do
        {read: { mode: :primary }}
      end

      let(:expected_read_preference) do
        {'mode' => 'primary'}
      end

      it_behaves_like 'sends expected read preference'
    end

    context 'when read preference is given in client and transaction options' do
      let(:client_options) do
        {read: { mode: :secondary }}
      end

      let(:tx_options) do
        {read: { mode: :primary }}
      end

      let(:expected_read_preference) do
        {'mode' => 'primary'}
      end

      it_behaves_like 'sends expected read preference'
    end

    context 'when read preference is given in collection and transaction options' do
      let(:collection) do
        client['tx_read_pref_test', {read: {mode: :secondary}}]
      end

      let(:tx_options) do
        {read: { mode: :primary }}
      end

      let(:expected_read_preference) do
        {'mode' => 'primary'}
      end

      it_behaves_like 'sends expected read preference'
    end

    context 'when read preference is given in transaction and operation options' do
      let(:tx_options) do
        {read: { mode: :primary }}
      end

      let(:find_options) do
        {read: {mode: :secondary}}
      end

      let(:expected_read_preference) do
        {'mode' => 'primary'}
      end

      it 'sends operation read preference and fails' do
        expect do
          session = client.start_session(session_options)
          session.with_transaction(tx_options) do
            collection.insert_one({hello: 'world'}, session: session)
            res = collection.find({}, {session: session}.merge(find_options || {})).to_a.count
            expect(res).to eq(1)
          end
        end.to raise_error(Mongo::Error::InvalidTransactionOperation, "read preference in a transaction must be primary (requested: secondary)")
      end
    end
  end
end
