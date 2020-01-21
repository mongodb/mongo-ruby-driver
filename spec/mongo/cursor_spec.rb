require 'spec_helper'

describe Mongo::Cursor do
  let(:authorized_collection) do
    authorized_client['cursor_spec_collection']
  end

  before do
    authorized_collection.drop
  end

  describe '#initialize' do
    let(:server) do
      view.send(:server_selector).select_server(authorized_client.cluster)
    end

    let(:reply) do
      view.send(:send_initial_query, server)
    end

    let(:cursor) do
      described_class.new(view, reply, server)
    end

    before do
      documents = [{test: 1}] * 10
      authorized_collection.insert_many(documents)
    end

    shared_context 'with initialized pool' do
      before do
        ClientRegistry.instance.close_all_clients

        # These tests really like creating pools (and thus scheduling
        # the pools' finalizers) when querying collections.
        # Deal with this by pre-creating pools for all known servers.
        cluster = authorized_collection.client.cluster
        cluster.next_primary
        cluster.servers_list.each do |server|
          server.pool
        end
      end
    end

    context 'cursor exhausted by initial result' do
      include_context 'with initialized pool'

      let(:view) do
        Mongo::Collection::View.new(authorized_collection)
      end

      it 'does not schedule the finalizer' do
        # Due to https://jira.mongodb.org/browse/RUBY-1772, restrict
        # the scope of the assertion
        RSpec::Mocks.with_temporary_scope do
          expect(ObjectSpace).not_to receive(:define_finalizer)
          cursor
        end
      end
    end

    context 'cursor not exhausted by initial result' do
      include_context 'with initialized pool'

      let(:view) do
        Mongo::Collection::View.new(authorized_collection, {}, batch_size: 2)
      end

      it 'schedules the finalizer' do
        # Due to https://jira.mongodb.org/browse/RUBY-1772, restrict
        # the scope of the assertion
        RSpec::Mocks.with_temporary_scope do
          expect(ObjectSpace).to receive(:define_finalizer)
          cursor
        end
      end
    end

    context 'server is unknown' do
      let(:server) do
        view.send(:server_selector).select_server(authorized_client.cluster).tap do |server|
          authorized_client.cluster.disconnect!
          server.unknown!
        end
      end

      let(:view) do
        Mongo::Collection::View.new(authorized_collection)
      end

      it 'works' do
        cursor
      end
    end
  end

  describe '#each' do

    let(:server) do
      view.send(:server_selector).select_server(authorized_client.cluster)
    end

    let(:reply) do
      view.send(:send_initial_query, server)
    end

    let(:cursor) do
      described_class.new(view, reply, server)
    end

    context 'when no options are provided to the view' do

      let(:view) do
        Mongo::Collection::View.new(authorized_collection)
      end

      context 'when the initial query retrieves all documents' do

        let(:documents) do
          (1..10).map{ |i| { field: "test#{i}" }}
        end

        before do
          authorized_collection.insert_many(documents)
        end

        it 'returns the correct amount' do
          expect(cursor.to_a.count).to eq(10)
        end

        it 'iterates the documents' do
          cursor.each do |doc|
            expect(doc).to have_key('field')
          end
        end
      end

      context 'when the initial query does not retrieve all documents' do

        let(:documents) do
          (1..102).map{ |i| { field: "test#{i}" }}
        end

        before do
          authorized_collection.insert_many(documents)
        end

        context 'when a getMore gets a socket error' do

          let(:op) do
            double('operation')
          end

          before do
            expect(cursor).to receive(:get_more_operation).and_return(op).ordered
            expect(op).to receive(:execute).and_raise(Mongo::Error::SocketError).ordered
          end

          it 'raises the error' do
            expect do
              cursor.each do |doc|
              end
            end.to raise_error(Mongo::Error::SocketError)
          end
        end

        context 'when no errors occur' do

          it 'returns the correct amount' do
            expect(cursor.to_a.count).to eq(102)
          end

          it 'iterates the documents' do
            cursor.each do |doc|
              expect(doc).to have_key('field')
            end
          end
        end
      end
    end

    context 'when options are provided to the view' do

      let(:documents) do
        (1..10).map{ |i| { field: "test#{i}" }}
      end

      before do
        authorized_collection.drop
        authorized_collection.insert_many(documents)
      end

      context 'when a limit is provided' do

        context 'when no batch size is provided' do

          context 'when the limit is positive' do

            let(:view) do
              Mongo::Collection::View.new(authorized_collection, {}, :limit => 2)
            end

            it 'returns the correct amount' do
              expect(cursor.to_a.count).to eq(2)
            end

            it 'iterates the documents' do
              cursor.each do |doc|
                expect(doc).to have_key('field')
              end
            end
          end

          context 'when the limit is negative' do

            let(:view) do
              Mongo::Collection::View.new(authorized_collection, {}, :limit => -2)
            end

            it 'returns the positive number of documents' do
              expect(cursor.to_a.count).to eq(2)
            end

            it 'iterates the documents' do
              cursor.each do |doc|
                expect(doc).to have_key('field')
              end
            end
          end

          context 'when the limit is zero' do

            let(:view) do
              Mongo::Collection::View.new(authorized_collection, {}, :limit => 0)
            end

            it 'returns all documents' do
              expect(cursor.to_a.count).to eq(10)
            end

            it 'iterates the documents' do
              cursor.each do |doc|
                expect(doc).to have_key('field')
              end
            end
          end
        end

        context 'when a batch size is provided' do

          context 'when the batch size is less than the limit' do

            let(:view) do
              Mongo::Collection::View.new(
                authorized_collection,
                {},
                :limit => 5, :batch_size => 3
              )
            end

            it 'returns the limited number of documents' do
              expect(cursor.to_a.count).to eq(5)
            end

            it 'iterates the documents' do
              cursor.each do |doc|
                expect(doc).to have_key('field')
              end
            end
          end

          context 'when the batch size is more than the limit' do

            let(:view) do
              Mongo::Collection::View.new(
                authorized_collection,
                {},
                :limit => 5, :batch_size => 7
              )
            end

            it 'returns the limited number of documents' do
              expect(cursor.to_a.count).to eq(5)
            end

            it 'iterates the documents' do
              cursor.each do |doc|
                expect(doc).to have_key('field')
              end
            end
          end

          context 'when the batch size is the same as the limit' do

            let(:view) do
              Mongo::Collection::View.new(
                authorized_collection,
                {},
                :limit => 5, :batch_size => 5
              )
            end

            it 'returns the limited number of documents' do
              expect(cursor.to_a.count).to eq(5)
            end

            it 'iterates the documents' do
              cursor.each do |doc|
                expect(doc).to have_key('field')
              end
            end
          end
        end
      end
    end

    context 'when the cursor is not fully iterated and is garbage collected' do

      let(:documents) do
        (1..6).map{ |i| { field: "test#{i}" }}
      end

      let(:cluster) do
        authorized_client.cluster
      end

      before do
        authorized_collection.insert_many(documents)
        cluster.schedule_kill_cursor(cursor.id,
                                     cursor.send(:kill_cursors_op_spec),
                                     cursor.instance_variable_get(:@server))
      end

      let(:view) do
        Mongo::Collection::View.new(
            authorized_collection,
            {},
            :batch_size => 2,
        )
      end

      let!(:cursor) do
        view.to_enum.next
        view.instance_variable_get(:@cursor)
      end

      it 'schedules a kill cursors op' do
        cluster.instance_variable_get(:@periodic_executor).flush
        expect do
          cursor.to_a
        end.to raise_exception(Mongo::Error::OperationFailure, /[cC]ursor.*not found/)
      end

      context 'when the cursor is unregistered before the kill cursors operations are executed' do
        # Sometimes JRuby yields 4 documents even though we are allowing
        # repeated cursor iteration below
        fails_on_jruby

        it 'does not send a kill cursors operation for the unregistered cursor' do
          # We need to verify that the cursor was able to retrieve more documents
          # from the server so that more than one batch is successfully received

          cluster.unregister_cursor(cursor.id)

          # The initial read is done on an enum obtained from the cursor.
          # The read below is done directly on the cursor. These are two
          # different objects. In MRI, iterating like this yields all of the
          # documents, hence we retrieved one document in the setup and
          # we expect to retrieve the remaining 5 here. In JRuby it appears that
          # the enum may buffers the first batch, such that the second document
          # sometimes is lost to the iteration and we retrieve 4 documents below.
          # But sometimes we get all 5 documents. In either case, all of the
          # documents are retrieved via two batches thus fulfilling the
          # requirement of the test to continue iterating the cursor.

=begin When repeated iteration of cursors is prohibited, these are the expectations
          if BSON::Environment.jruby?
            expected_counts = [4, 5]
          else
            expected_counts = [5]
          end
=end

          # Since currently repeated iteration of cursors is allowed, calling
          # to_a on the cursor would perform such an iteration and return
          # all documents of the initial read.
          expected_counts = [6]

          expect(expected_counts).to include(cursor.to_a.size)
        end
      end
    end

    context 'when the cursor is fully iterated' do

      let(:documents) do
        (1..3).map{ |i| { field: "test#{i}" }}
      end

      before do
        authorized_collection.delete_many
        authorized_collection.insert_many(documents)
      end

      let(:view) do
        authorized_collection.find({}, batch_size: 2)
      end

      let(:cursor) do
        view.instance_variable_get(:@cursor)
      end

      let!(:cursor_id) do
        enum.next
        enum.next
        cursor.id
      end

      let(:enum) do
        view.to_enum
      end

      let(:cursor_reaper) do
        authorized_collection.client.cluster.instance_variable_get(:@cursor_reaper)
      end

      it 'removes the cursor id from the active cursors tracked by the cluster cursor manager' do
        enum.next
        expect(cursor_reaper.instance_variable_get(:@active_cursors)).not_to include(cursor_id)
      end
    end
  end

  context 'when an implicit session is used' do
    min_server_fcv '3.6'

    let(:collection) do
      subscribed_client[TEST_COLL]
    end

    before do
      collection.insert_many(documents)
    end

    let(:cursor) do
      view.instance_variable_get(:@cursor)
    end

    let(:enum) do
      view.to_enum
    end

    let(:session_pool_ids) do
      queue = view.client.cluster.session_pool.instance_variable_get(:@queue)
      queue.collect { |s| s.session_id }
    end

    let(:find_events) do
      EventSubscriber.started_events.select { |e| e.command_name == "find" }
    end

    context 'when all results are retrieved in the first response' do

      let(:documents) do
        (1..2).map{ |i| { field: "test#{i}" }}
      end

      let(:view) do
        collection.find
      end

      it 'returns the session to the cluster session pool' do
        1.times { enum.next }
        expect(find_events.collect { |event| event.command['lsid'] }.uniq.size).to eq(1)
        expect(session_pool_ids).to include(find_events.collect { |event| event.command['lsid'] }.uniq.first)
      end
    end

    context 'when a getMore is needed to retrieve all results' do
      min_server_fcv '3.6'
      require_topology :single, :replica_set

      let(:documents) do
        (1..4).map{ |i| { field: "test#{i}" }}
      end

      let(:view) do
        collection.find({}, batch_size: 2, limit: 4)
      end


      context 'when not all documents are iterated' do

        it 'returns the session to the cluster session pool' do
          3.times { enum.next }
          expect(find_events.collect { |event| event.command['lsid'] }.uniq.size).to eq(1)
          expect(session_pool_ids).to include(find_events.collect { |event| event.command['lsid'] }.uniq.first)
        end
      end

      context 'when all documents are iterated' do

        it 'returns the session to the cluster session pool' do
          4.times { enum.next }
          expect(find_events.collect { |event| event.command['lsid'] }.uniq.size).to eq(1)
          expect(session_pool_ids).to include(find_events.collect { |event| event.command['lsid'] }.uniq.first)
        end
      end
    end
  end

  describe '#inspect' do

    let(:view) do
      Mongo::Collection::View.new(authorized_collection)
    end

    let(:query_spec) do
      { selector: {}, options: {},
        db_name: SpecConfig.instance.test_db, coll_name: TEST_COLL }
    end

    let(:reply) do
      Mongo::Operation::Find.new(query_spec).tap do |reply|
        allow(reply).to receive(:cursor_id).and_return(42)
      end
    end

    let(:cursor) do
      described_class.new(view, reply, authorized_primary)
    end

    it 'returns a string' do
      expect(cursor.inspect).to be_a(String)
    end

    it 'returns a string containing the collection view inspect string' do
      expect(cursor.inspect).to match(/.*#{view.inspect}.*/)
    end
  end

  describe '#to_a' do

    let(:view) do
      Mongo::Collection::View.new(authorized_collection, {}, batch_size: 10)
    end

    let(:query_spec) do
      { :selector => {}, :options => {}, :db_name => SpecConfig.instance.test_db,
        :coll_name => authorized_collection.name }
    end

    let(:reply) do
      view.send(:send_initial_query, authorized_primary)
    end

    let(:cursor) do
      described_class.new(view, reply, authorized_primary)
    end

    context 'after partially iterating the cursor' do
      before do
        authorized_collection.drop
        docs = []
        100.times do |i|
          docs << {a: i}
        end
        authorized_collection.insert_many(docs)
      end

      context 'after #each was called once' do
        before do
          cursor.each do |doc|
            break
          end
        end

        it 'iterates from the beginning of the view' do
          expect(cursor.to_a.map { |doc| doc['a'] }).to eq((0..99).to_a)
        end
      end

      context 'after exactly one batch was iterated' do
        before do
          cursor.each_with_index do |doc, i|
            break if i == 9
          end
        end

        it 'iterates from the beginning of the view' do
          expect(cursor.to_a.map { |doc| doc['a'] }).to eq((0..99).to_a)
        end
      end

      context 'after two batches were iterated' do
        before do
          cursor.each_with_index do |doc, i|
            break if i == 19
          end
        end

=begin Behavior of pre-2.10 driver:
        it 'skips the second batch' do
          expect(cursor.to_a.map { |doc| doc['a'] }).to eq((0..9).to_a + (20..99).to_a)
        end
=end
        it 'raises InvalidCursorOperation' do
          expect do
            cursor.to_a
          end.to raise_error(Mongo::Error::InvalidCursorOperation, 'Cannot restart iteration of a cursor which issued a getMore')
        end
      end
    end
  end
end
