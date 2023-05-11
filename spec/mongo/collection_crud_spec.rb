# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Collection do

  let(:subscriber) { Mrss::EventSubscriber.new }

  let(:client) do
    authorized_client.tap do |client|
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    end
  end

  let(:authorized_collection) { client['collection_spec'] }

  before do
    authorized_client['collection_spec'].drop
  end

  let(:collection_invalid_write_concern) do
    authorized_collection.client.with(write: INVALID_WRITE_CONCERN)[authorized_collection.name]
  end

  let(:collection_with_validator) do
    authorized_client[:validating]
  end

  describe '#find' do

    describe 'updating cluster time' do

      let(:operation) do
        client[TEST_COLL].find.first
      end

      let(:operation_with_session) do
        client[TEST_COLL].find({}, session: session).first
      end

      let(:second_operation) do
        client[TEST_COLL].find({}, session: session).first
      end

      it_behaves_like 'an operation updating cluster time'
    end

    context 'when provided a filter' do

      let(:view) do
        authorized_collection.find(name: 1)
      end

      it 'returns a authorized_collection view for the filter' do
        expect(view.filter).to eq('name' => 1)
      end
    end

    context 'when provided no filter' do

      let(:view) do
        authorized_collection.find
      end

      it 'returns a authorized_collection view with an empty filter' do
        expect(view.filter).to be_empty
      end
    end

    context 'when providing a bad filter' do

      let(:view) do
        authorized_collection.find('$or' => [])
      end

      it 'raises an exception when iterating' do
        expect {
          view.to_a
        }.to raise_exception(Mongo::Error::OperationFailure)
      end
    end

    context 'when iterating the authorized_collection view' do

      before do
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test2' }])
      end

      let(:view) do
        authorized_collection.find
      end

      it 'iterates over the documents' do
        view.each do |document|
          expect(document).to_not be_nil
        end
      end
    end

    context 'when the user is not authorized' do
      require_auth

      let(:view) do
        unauthorized_collection.find
      end

      it 'iterates over the documents' do
        expect {
          view.each{ |document| document }
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end

    context 'when documents contain potential error message fields' do

      [ 'errmsg', 'error', Mongo::Operation::Result::OK ].each do |field|

        context "when the document contains a '#{field}' field" do

          let(:value) do
            'testing'
          end

          let(:view) do
            authorized_collection.find
          end

          before do
            authorized_collection.insert_one({ field => value })
          end

          it 'iterates over the documents' do
            view.each do |document|
              expect(document[field]).to eq(value)
            end
          end
        end
      end
    end

    context 'when provided options' do

      context 'when a session is provided' do
        require_wired_tiger

        let(:operation) do
          authorized_collection.find({}, session: session).to_a
        end

        let(:session) do
          authorized_client.start_session
        end

        let(:failed_operation) do
          client[authorized_collection.name].find({ '$._id' => 1 }, session: session).to_a
        end

        let(:client) do
          authorized_client
        end

        it_behaves_like 'an operation using a session'
        it_behaves_like 'a failed operation using a session'
      end

      context 'session id' do
        min_server_fcv '3.6'
        require_topology :replica_set, :sharded
        require_wired_tiger

        let(:options) do
          { session: session }
        end

        let(:session) do
          client.start_session
        end

        let(:view) do
          Mongo::Collection::View.new(client[TEST_COLL], selector, view_options)
        end

        let(:command) do
          client[TEST_COLL].find({}, session: session).explain
          subscriber.started_events.find { |c| c.command_name == 'explain' }.command
        end

        it 'sends the session id' do
          expect(command['lsid']).to eq(session.session_id)
        end
      end

      context 'when a session supporting causal consistency is used' do
        require_wired_tiger

        let(:operation) do
          collection.find({}, session: session).to_a
        end

        let(:command) do
          operation
          subscriber.started_events.find { |cmd| cmd.command_name == 'find' }.command
        end

        it_behaves_like 'an operation supporting causally consistent reads'
      end

      let(:view) do
        authorized_collection.find({}, options)
      end

      context 'when provided :allow_partial_results' do

        let(:options) do
          { allow_partial_results: true }
        end

        it 'returns a view with :allow_partial_results set' do
          expect(view.options[:allow_partial_results]).to be(options[:allow_partial_results])
        end
      end

      context 'when provided :batch_size' do

        let(:options) do
          { batch_size: 100 }
        end

        it 'returns a view with :batch_size set' do
          expect(view.options[:batch_size]).to eq(options[:batch_size])
        end
      end

      context 'when provided :comment' do

        let(:options) do
          { comment: 'slow query' }
        end

        it 'returns a view with :comment set' do
          expect(view.modifiers[:$comment]).to eq(options[:comment])
        end
      end

      context 'when provided :cursor_type' do

        let(:options) do
          { cursor_type: :tailable }
        end

        it 'returns a view with :cursor_type set' do
          expect(view.options[:cursor_type]).to eq(options[:cursor_type])
        end
      end

      context 'when provided :max_time_ms' do

        let(:options) do
          { max_time_ms: 500 }
        end

        it 'returns a view with :max_time_ms set' do
          expect(view.modifiers[:$maxTimeMS]).to eq(options[:max_time_ms])
        end
      end

      context 'when provided :modifiers' do

        let(:options) do
          { modifiers: { '$orderby' => Mongo::Index::ASCENDING } }
        end

        it 'returns a view with modifiers set' do
          expect(view.modifiers).to eq(options[:modifiers])
        end

        it 'dups the modifiers hash' do
          expect(view.modifiers).not_to be(options[:modifiers])
        end
      end

      context 'when provided :no_cursor_timeout' do

        let(:options) do
          { no_cursor_timeout: true }
        end

        it 'returns a view with :no_cursor_timeout set' do
          expect(view.options[:no_cursor_timeout]).to eq(options[:no_cursor_timeout])
        end
      end

      context 'when provided :oplog_replay' do

        let(:options) do
          { oplog_replay: false }
        end

        it 'returns a view with :oplog_replay set' do
          expect(view.options[:oplog_replay]).to eq(options[:oplog_replay])
        end
      end

      context 'when provided :projection' do

        let(:options) do
          { projection:  { 'x' => 1 } }
        end

        it 'returns a view with :projection set' do
          expect(view.options[:projection]).to eq(options[:projection])
        end
      end

      context 'when provided :skip' do

        let(:options) do
          { skip:  5 }
        end

        it 'returns a view with :skip set' do
          expect(view.options[:skip]).to eq(options[:skip])
        end
      end

      context 'when provided :sort' do

        let(:options) do
          { sort:  { 'x' => Mongo::Index::ASCENDING } }
        end

        it 'returns a view with :sort set' do
          expect(view.modifiers[:$orderby]).to eq(options[:sort])
        end
      end

      context 'when provided :collation' do

        let(:options) do
          { collation: { 'locale' => 'en_US' } }
        end

        it 'returns a view with :collation set' do
          expect(view.options[:collation]).to eq(options[:collation])
        end
      end
    end
  end

  describe '#insert_many' do

    let(:result) do
      authorized_collection.insert_many([{ name: 'test1' }, { name: 'test2' }])
    end

    it 'inserts the documents into the collection' do
      expect(result.inserted_count).to eq(2)
    end

    it 'contains the ids in the result' do
      expect(result.inserted_ids.size).to eq(2)
    end

    context 'when an enumerable is used instead of an array' do

      context 'when the enumerable is not empty' do

        let(:source_data) do
          [{ name: 'test1' }, { name: 'test2' }]
        end

        let(:result) do
          authorized_collection.insert_many(source_data.lazy)
        end

        it 'should accepts them without raising an error' do
          expect { result }.to_not raise_error
          expect(result.inserted_count).to eq(source_data.size)
        end
      end

      context 'when the enumerable is empty' do

        let(:source_data) do
          []
        end

        let(:result) do
          authorized_collection.insert_many(source_data.lazy)
        end

        it 'should raise ArgumentError' do
          expect do
            result
          end.to raise_error(ArgumentError, /Bulk write requests cannot be empty/)
        end
      end
    end

    context 'when a session is provided' do

      let(:session) do
        authorized_client.start_session
      end

      let(:operation) do
        authorized_collection.insert_many([{ name: 'test1' }, { name: 'test2' }], session: session)
      end

      let(:failed_operation) do
        authorized_collection.insert_many([{ _id: 'test1' }, { _id: 'test1' }], session: session)
      end

      let(:client) do
        authorized_client
      end

      it_behaves_like 'an operation using a session'
      it_behaves_like 'a failed operation using a session'
    end

    context 'when unacknowledged writes is used with an explicit session' do

      let(:collection_with_unacknowledged_write_concern) do
        authorized_collection.with(write: { w: 0 })
      end

      let(:operation) do
        collection_with_unacknowledged_write_concern.insert_many([{ name: 'test1' }, { name: 'test2' }], session: session)
      end

      it_behaves_like 'an explicit session with an unacknowledged write'
    end

    context 'when unacknowledged writes is used with an implicit session' do

      let(:collection_with_unacknowledged_write_concern) do
        client.with(write: { w: 0 })[TEST_COLL]
      end

      let(:operation) do
        collection_with_unacknowledged_write_concern.insert_many([{ name: 'test1' }, { name: 'test2' }])
      end

      it_behaves_like 'an implicit session with an unacknowledged write'
    end

    context 'when a document contains dotted keys' do

      let(:docs) do
        [ { 'first.name' => 'test1' }, { name: 'test2' } ]
      end

      let(:view) { authorized_collection.find({}, { sort: { name: 1 } }) }

      it 'inserts the documents correctly' do
        expect {
          authorized_collection.insert_many(docs)
        }.to_not raise_error

        expect(view.count).to eq(2)
        expect(view.first['first.name']).to eq('test1')
        expect(view.to_a[1]['name']).to eq('test2')
      end
    end

    context 'when the client has a custom id generator' do

      let(:generator) do
        Class.new do
          def generate
            1
          end
        end.new
      end

      let(:custom_client) do
        authorized_client.with(id_generator: generator)
      end

      let(:custom_collection) do
        custom_client['custom_id_generator_test_collection']
      end

      before do
        custom_collection.delete_many
        custom_collection.insert_many([{ name: 'testing' }])
        expect(custom_collection.count).to eq(1)
      end

      it 'inserts with the custom id' do
        expect(custom_collection.count).to eq(1)
        expect(custom_collection.find.first[:_id]).to eq(1)
      end
    end

    context 'when the inserts fail' do

      let(:result) do
        authorized_collection.insert_many([{ _id: 1 }, { _id: 1 }])
      end

      it 'raises an BulkWriteError' do
        expect {
          result
        }.to raise_exception(Mongo::Error::BulkWriteError)
      end
    end

    context "when the documents exceed the max bson size" do

      let(:documents) do
        [{ '_id' => 1, 'name' => '1'*17000000 }]
      end

      it 'raises a MaxBSONSize error' do
        expect {
          authorized_collection.insert_many(documents)
        }.to raise_error(Mongo::Error::MaxBSONSize)
      end
    end

    context 'when the documents are sent with OP_MSG' do
      min_server_fcv '3.6'

      let(:documents) do
        [{ '_id' => 1, 'name' => '1'*16777191 }, { '_id' => 'y' }]
      end

      before do
        authorized_collection.insert_many(documents)
      end

      let(:insert_events) do
        subscriber.started_events.select { |e| e.command_name == 'insert' }
      end

      it 'sends the documents in one OP_MSG' do
        expect(insert_events.size).to eq(1)
        expect(insert_events[0].command['documents']).to eq(documents)
      end
    end

    context 'when collection has a validator' do
      min_server_fcv '3.2'

      around(:each) do |spec|
        authorized_client[:validating].drop
        authorized_client[:validating,
                          :validator => { :a => { '$exists' => true } }].tap do |c|
          c.create
        end
        spec.run
        collection_with_validator.drop
      end

      context 'when the document is valid' do

        let(:result) do
          collection_with_validator.insert_many([{ a: 1 }, { a: 2 }])
        end

        it 'inserts successfully' do
          expect(result.inserted_count).to eq(2)
        end
      end

      context 'when the document is invalid' do

        context 'when bypass_document_validation is not set' do

          let(:result2) do
            collection_with_validator.insert_many([{ x: 1 }, { x: 2 }])
          end

          it 'raises a BulkWriteError' do
            expect {
              result2
            }.to raise_exception(Mongo::Error::BulkWriteError)
          end
        end

        context 'when bypass_document_validation is true' do

          let(:result3) do
            collection_with_validator.insert_many(
              [{ x: 1 }, { x: 2 }], :bypass_document_validation => true)
          end

          it 'inserts successfully' do
            expect(result3.inserted_count).to eq(2)
          end
        end
      end
    end

    context 'when unacknowledged writes is used' do

      let(:collection_with_unacknowledged_write_concern) do
        authorized_collection.with(write: { w: 0 })
      end

      let(:result) do
        collection_with_unacknowledged_write_concern.insert_many([{ _id: 1 }, { _id: 1 }])
      end

      it 'does not raise an exception' do
        expect(result.inserted_count).to be(0)
      end
    end

    context 'when various options passed in' do
      # w: 2 requires a replica set
      require_topology :replica_set

      # https://jira.mongodb.org/browse/RUBY-2306
      min_server_fcv '3.6'

      let(:session) do
        authorized_client.start_session
      end

      let(:events) do
        subscriber.command_started_events('insert')
      end

      let(:collection) do
        authorized_collection.with(write_concern: {w: 2})
      end

      let!(:command) do
        Utils.get_command_event(authorized_client, 'insert') do |client|
          collection.insert_many([{ name: 'test1' }, { name: 'test2' }], session: session,
            write_concern: {w: 1}, bypass_document_validation: true)
        end.command
      end

      it 'inserts many successfully with correct options sent to server' do
        expect(events.length).to eq(1)
        expect(command[:writeConcern]).to_not be_nil
        expect(command[:writeConcern][:w]).to eq(1)
        expect(command[:bypassDocumentValidation]).to be(true)
      end
    end
  end

  describe '#insert_one' do

    describe 'updating cluster time' do

      let(:operation) do
        client[TEST_COLL].insert_one({ name: 'testing' })
      end

      let(:operation_with_session) do
        client[TEST_COLL].insert_one({ name: 'testing' }, session: session)
      end

      let(:second_operation) do
        client[TEST_COLL].insert_one({ name: 'testing' }, session: session)
      end

      it_behaves_like 'an operation updating cluster time'
    end

    let(:result) do
      authorized_collection.insert_one({ name: 'testing' })
    end

    it 'inserts the document into the collection'do
      expect(result.written_count).to eq(1)
    end

    it 'contains the id in the result' do
      expect(result.inserted_id).to_not be_nil
    end

    context 'when a session is provided' do

      let(:session) do
        authorized_client.start_session
      end

      let(:operation) do
        authorized_collection.insert_one({ name: 'testing' }, session: session)
      end

      let(:failed_operation) do
        authorized_collection.insert_one({ _id: 'testing' })
        authorized_collection.insert_one({ _id: 'testing' }, session: session)
      end

      let(:client) do
        authorized_client
      end

      it_behaves_like 'an operation using a session'
      it_behaves_like 'a failed operation using a session'
    end

    context 'when unacknowledged writes is used with an explicit session' do

      let(:collection_with_unacknowledged_write_concern) do
        authorized_collection.with(write: { w: 0 })
      end

      let(:operation) do
        collection_with_unacknowledged_write_concern.insert_one({ name: 'testing' }, session: session)
      end

      it_behaves_like 'an explicit session with an unacknowledged write'
    end

    context 'when unacknowledged writes is used with an implicit session' do

      let(:collection_with_unacknowledged_write_concern) do
        client.with(write: { w: 0 })[TEST_COLL]
      end

      let(:operation) do
        collection_with_unacknowledged_write_concern.insert_one({ name: 'testing' })
      end

      it_behaves_like 'an implicit session with an unacknowledged write'
    end

    context 'when various options passed in' do
      # https://jira.mongodb.org/browse/RUBY-2306
      min_server_fcv '3.6'

      let(:session) do
        authorized_client.start_session
      end

      let(:events) do
        subscriber.command_started_events('insert')
      end

      let(:collection) do
        authorized_collection.with(write_concern: {w: 3})
      end

      let!(:command) do
        Utils.get_command_event(authorized_client, 'insert') do |client|
          collection.insert_one({name: 'test1'}, session: session, write_concern: {w: 1},
            bypass_document_validation: true)
        end.command
      end

      it 'inserts one successfully with correct options sent to server' do
        expect(events.length).to eq(1)
        expect(command[:writeConcern]).to_not be_nil
        expect(command[:writeConcern][:w]).to eq(1)
        expect(command[:bypassDocumentValidation]).to be(true)
      end
    end

    context 'when the document contains dotted keys' do

      let(:doc) do
        { 'testing.test' => 'value' }
      end

      it 'inserts the document correctly' do
        expect {
          authorized_collection.insert_one(doc)
        }.to_not raise_error

        expect(authorized_collection.count).to eq(1)
        expect(authorized_collection.find.first['testing.test']).to eq('value')
      end
    end

    context 'when the document is nil' do
      let(:result) do
        authorized_collection.insert_one(nil)
      end

      it 'raises an ArgumentError' do
       expect {
         result
       }.to raise_error(ArgumentError, "Document to be inserted cannot be nil")
     end
    end

    context 'when the insert fails' do

      let(:result) do
        authorized_collection.insert_one(_id: 1)
        authorized_collection.insert_one(_id: 1)
      end

      it 'raises an OperationFailure' do
        expect {
          result
        }.to raise_exception(Mongo::Error::OperationFailure)
      end
    end

    context 'when the client has a custom id generator' do

      let(:generator) do
        Class.new do
          def generate
            1
          end
        end.new
      end

      let(:custom_client) do
        authorized_client.with(id_generator: generator)
      end

      let(:custom_collection) do
        custom_client[TEST_COLL]
      end

      before do
        custom_collection.delete_many
        custom_collection.insert_one({ name: 'testing' })
      end

      it 'inserts with the custom id' do
        expect(custom_collection.find.first[:_id]).to eq(1)
      end
    end

    context 'when collection has a validator' do
      min_server_fcv '3.2'

      around(:each) do |spec|
        authorized_client[:validating,
                          :validator => { :a => { '$exists' => true } }].tap do |c|
          c.create
        end
        spec.run
        collection_with_validator.drop
      end

      context 'when the document is valid' do

        let(:result) do
          collection_with_validator.insert_one({ a: 1 })
        end

        it 'inserts successfully' do
          expect(result.written_count).to eq(1)
        end
      end

      context 'when the document is invalid' do

        context 'when bypass_document_validation is not set' do

          let(:result2) do
            collection_with_validator.insert_one({ x: 1 })
          end

          it 'raises a OperationFailure' do
            expect {
              result2
            }.to raise_exception(Mongo::Error::OperationFailure)
          end
        end

        context 'when bypass_document_validation is true' do

          let(:result3) do
            collection_with_validator.insert_one(
              { x: 1 }, :bypass_document_validation => true)
          end

          it 'inserts successfully' do
            expect(result3.written_count).to eq(1)
          end
        end
      end
    end
  end

  describe '#bulk_write' do

    context 'when various options passed in' do
      min_server_fcv '3.2'
      require_topology :replica_set

      # https://jira.mongodb.org/browse/RUBY-2306
      min_server_fcv '3.6'

      let(:requests) do
        [
          { insert_one: { name: "anne" }},
          { insert_one: { name: "bob" }},
          { insert_one: { name: "charlie" }}
        ]
      end

      let(:session) do
        authorized_client.start_session
      end

      let!(:command) do
        Utils.get_command_event(authorized_client, 'insert') do |client|
          collection.bulk_write(requests, session: session, write_concern: {w: 1},
            bypass_document_validation: true)
        end.command
      end

      let(:events) do
        subscriber.command_started_events('insert')
      end

      let(:collection) do
        authorized_collection.with(write_concern: {w: 2})
      end

      it 'inserts successfully with correct options sent to server' do
        expect(collection.count).to eq(3)
        expect(events.length).to eq(1)
        expect(command[:writeConcern]).to_not be_nil
        expect(command[:writeConcern][:w]).to eq(1)
        expect(command[:bypassDocumentValidation]).to eq(true)
      end
    end
  end

  describe '#aggregate' do

    describe 'updating cluster time' do

      let(:operation) do
        client[TEST_COLL].aggregate([]).first
      end

      let(:operation_with_session) do
        client[TEST_COLL].aggregate([], session: session).first
      end

      let(:second_operation) do
        client[TEST_COLL].aggregate([], session: session).first
      end

      it_behaves_like 'an operation updating cluster time'
    end

    context 'when a session supporting causal consistency is used' do
      require_wired_tiger

      let(:operation) do
        collection.aggregate([], session: session).first
      end

      let(:command) do
        operation
        subscriber.started_events.find { |cmd| cmd.command_name == 'aggregate' }.command
      end

      it_behaves_like 'an operation supporting causally consistent reads'
    end

    it 'returns an Aggregation object' do
      expect(authorized_collection.aggregate([])).to be_a(Mongo::Collection::View::Aggregation)
    end

    context 'when options are provided' do

      let(:options) do
        { :allow_disk_use => true, :bypass_document_validation => true }
      end

      it 'sets the options on the Aggregation object' do
        expect(authorized_collection.aggregate([], options).options).to eq(BSON::Document.new(options))
      end

      context 'when the :comment option is provided' do

        let(:options) do
          { :comment => 'testing' }
        end

        it 'sets the options on the Aggregation object' do
          expect(authorized_collection.aggregate([], options).options).to eq(BSON::Document.new(options))
        end
      end

      context 'when a session is provided' do

        let(:session) do
          authorized_client.start_session
        end

        let(:operation) do
          authorized_collection.aggregate([], session: session).to_a
        end

        let(:failed_operation) do
          authorized_collection.aggregate([ { '$invalid' => 1 }], session: session).to_a
        end

        let(:client) do
          authorized_client
        end

        it_behaves_like 'an operation using a session'
        it_behaves_like 'a failed operation using a session'
      end

      context 'when a hint is provided' do

        let(:options) do
          { 'hint' => { 'y' => 1 } }
        end

        it 'sets the options on the Aggregation object' do
          expect(authorized_collection.aggregate([], options).options).to eq(options)
        end
      end

      context 'when collation is provided' do

        before do
          authorized_collection.insert_many([ { name: 'bang' }, { name: 'bang' }])
        end

        let(:pipeline) do
          [{ "$match" => { "name" => "BANG" } }]
        end

        let(:options) do
          { collation: { locale: 'en_US', strength: 2 } }
        end

        let(:result) do
          authorized_collection.aggregate(pipeline, options).collect { |doc| doc['name']}
        end

        context 'when the server selected supports collations' do
          min_server_fcv '3.4'

          it 'applies the collation' do
            expect(result).to eq(['bang', 'bang'])
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

  describe '#count_documents' do

    before do
      authorized_collection.delete_many
    end

    context 'no argument provided' do

      context 'when collection is empty' do
        it 'returns 0 matching documents' do
          expect(authorized_collection.count_documents).to eq(0)
        end
      end

      context 'when collection is not empty' do

        let(:documents) do
          documents = []
          1.upto(10) do |index|
            documents << { key: 'a', _id: "in#{index}" }
          end
          documents
        end

        before do
          authorized_collection.insert_many(documents)
        end

        it 'returns 10 matching documents' do
          expect(authorized_collection.count_documents).to eq(10)
        end
      end
    end

    context 'when transactions are enabled' do
      require_wired_tiger
      require_transaction_support

      before do
        # Ensure that the collection is created
        authorized_collection.insert_one(x: 1)
        authorized_collection.delete_many({})
      end

      let(:session) do
        authorized_client.start_session
      end

      it 'successfully starts a transaction and executes a transaction' do
        session.start_transaction
        expect(
          session.instance_variable_get(:@state)
        ).to eq(Mongo::Session::STARTING_TRANSACTION_STATE)

        expect(authorized_collection.count_documents({}, { session: session })).to eq(0)
        expect(
          session.instance_variable_get(:@state)
        ).to eq(Mongo::Session::TRANSACTION_IN_PROGRESS_STATE)

        authorized_collection.insert_one({ x: 1 }, { session: session })
        expect(authorized_collection.count_documents({}, { session: session })).to eq(1)

        session.commit_transaction
        expect(
          session.instance_variable_get(:@state)
        ).to eq(Mongo::Session::TRANSACTION_COMMITTED_STATE)
      end
    end
  end

  describe '#count' do

    let(:documents) do
      (1..10).map{ |i| { field: "test#{i}" }}
    end

    before do
      authorized_collection.insert_many(documents)
    end

    it 'returns an integer count' do
      expect(authorized_collection.count).to eq(10)
    end

    context 'when options are provided' do

      it 'passes the options to the count' do
        expect(authorized_collection.count({}, limit: 5)).to eq(5)
      end

      context 'when a session is provided' do
        require_wired_tiger

        let(:session) do
          authorized_client.start_session
        end

        let(:operation) do
          authorized_collection.count({}, session: session)
        end

        let(:failed_operation) do
          authorized_collection.count({ '$._id' => 1 }, session: session)
        end

        let(:client) do
          authorized_client
        end

        it_behaves_like 'an operation using a session'
        it_behaves_like 'a failed operation using a session'
      end

      context 'when a session supporting causal consistency is used' do
        require_wired_tiger

        let(:operation) do
          collection.count({}, session: session)
        end

        let(:command) do
          operation
          subscriber.started_events.find { |cmd| cmd.command_name == 'count' }.command
        end

        it_behaves_like 'an operation supporting causally consistent reads'
      end

      context 'when a collation is specified' do

        let(:selector) do
          { name: 'BANG' }
        end

        let(:result) do
          authorized_collection.count(selector, options)
        end

        before do
          authorized_collection.insert_one(name: 'bang')
        end

        let(:options) do
          { collation: { locale: 'en_US', strength: 2 } }
        end

        context 'when the server selected supports collations' do
          min_server_fcv '3.4'

          it 'applies the collation to the count' do
            expect(result).to eq(1)
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

  describe '#distinct' do

    let(:documents) do
      (1..3).map{ |i| { field: "test#{i}" }}
    end

    before do
      authorized_collection.insert_many(documents)
    end

    it 'returns the distinct values' do
      expect(authorized_collection.distinct(:field).sort).to eq([ 'test1', 'test2', 'test3' ])
    end

    context 'when a selector is provided' do

      it 'returns the distinct values' do
        expect(authorized_collection.distinct(:field, field: 'test1')).to eq([ 'test1' ])
      end
    end

    context 'when options are provided' do

      it 'passes the options to the distinct command' do
        expect(authorized_collection.distinct(:field, {}, max_time_ms: 100).sort).to eq([ 'test1', 'test2', 'test3' ])
      end

      context 'when a session is provided' do
        require_wired_tiger

        let(:session) do
          authorized_client.start_session
        end

        let(:operation) do
          authorized_collection.distinct(:field, {}, session: session)
        end

        let(:failed_operation) do
          authorized_collection.distinct(:field, { '$._id' => 1 }, session: session)
        end

        let(:client) do
          authorized_client
        end

        it_behaves_like 'an operation using a session'
        it_behaves_like 'a failed operation using a session'
      end
    end

    context 'when a session supporting causal consistency is used' do
      require_wired_tiger

      let(:operation) do
        collection.distinct(:field, {}, session: session)
      end

      let(:command) do
        operation
        subscriber.started_events.find { |cmd| cmd.command_name == 'distinct' }.command
      end

      it_behaves_like 'an operation supporting causally consistent reads'
    end

    context 'when a collation is specified' do

      let(:result) do
        authorized_collection.distinct(:name, {}, options)
      end

      before do
        authorized_collection.insert_one(name: 'bang')
        authorized_collection.insert_one(name: 'BANG')
      end

      let(:options) do
        { collation: { locale: 'en_US', strength: 2 } }
      end

      context 'when the server selected supports collations' do
        min_server_fcv '3.4'

        it 'applies the collation to the distinct' do
          expect(result).to eq(['bang'])
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

    context 'when a collation is not specified' do

      let(:result) do
        authorized_collection.distinct(:name)
      end

      before do
        authorized_collection.insert_one(name: 'bang')
        authorized_collection.insert_one(name: 'BANG')
      end

      it 'does not apply the collation to the distinct' do
        expect(result).to match_array(['bang', 'BANG'])
      end
    end
  end

  describe '#delete_one' do

    context 'when a selector was provided' do

      let(:selector) do
        { field: 'test1' }
      end

      before do
        authorized_collection.insert_many([
                                            { field: 'test1' },
                                            { field: 'test1' },
                                            { field: 'test1' }
                                          ])
      end

      let(:response) do
        authorized_collection.delete_one(selector)
      end

      it 'deletes the first matching document in the collection' do
        expect(response.deleted_count).to eq(1)
      end
    end

    context 'when no selector was provided' do

      before do
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test2' }])
      end

      let(:response) do
        authorized_collection.delete_one
      end

      it 'deletes the first document in the collection' do
        expect(response.deleted_count).to eq(1)
      end
    end

    context 'when the delete fails' do
      require_topology :single

      let(:result) do
        collection_invalid_write_concern.delete_one
      end

      it 'raises an OperationFailure' do
        expect {
          result
        }.to raise_exception(Mongo::Error::OperationFailure)
      end
    end

    context 'when a session is provided' do

      let(:session) do
        authorized_client.start_session
      end

      let(:operation) do
        authorized_collection.delete_one({}, session: session)
      end

      let(:failed_operation) do
        authorized_collection.delete_one({ '$._id' => 1}, session: session)
      end

      let(:client) do
        authorized_client
      end

      it_behaves_like 'an operation using a session'
      it_behaves_like 'a failed operation using a session'
    end

    context 'when unacknowledged writes is used' do

      let(:collection_with_unacknowledged_write_concern) do
        authorized_collection.with(write: { w: 0 })
      end

      let(:operation) do
        collection_with_unacknowledged_write_concern.delete_one({}, session: session)
      end

      it_behaves_like 'an explicit session with an unacknowledged write'
    end

    context 'when unacknowledged writes is used with an implicit session' do

      let(:collection_with_unacknowledged_write_concern) do
        client.with(write: { w: 0 })[TEST_COLL]
      end

      let(:operation) do
        collection_with_unacknowledged_write_concern.delete_one
      end

      it_behaves_like 'an implicit session with an unacknowledged write'
    end

    context 'when a collation is provided' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        authorized_collection.delete_one(selector, options)
      end

      before do
        authorized_collection.insert_one(name: 'bang')
      end

      let(:options) do
        { collation: { locale: 'en_US', strength: 2 } }
      end

      context 'when the server selected supports collations' do
        min_server_fcv '3.4'

        it 'applies the collation' do
          expect(result.written_count).to eq(1)
          expect(authorized_collection.find(name: 'bang').count).to eq(0)
        end

        context 'when unacknowledged writes is used' do

          let(:collection_with_unacknowledged_write_concern) do
            authorized_collection.with(write: { w: 0 })
          end

          let(:result) do
            collection_with_unacknowledged_write_concern.delete_one(selector, options)
          end

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

    context 'when collation is not specified' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        authorized_collection.delete_one(selector)
      end

      before do
        authorized_collection.insert_one(name: 'bang')
      end

      it 'does not apply the collation' do
        expect(result.written_count).to eq(0)
        expect(authorized_collection.find(name: 'bang').count).to eq(1)
      end
    end

    context 'when various options passed in' do
      # w: 2 requires a replica set
      require_topology :replica_set

      # https://jira.mongodb.org/browse/RUBY-2306
      min_server_fcv '3.6'

      before do
        authorized_collection.insert_many([{ name: 'test1' }, { name: 'test2' }])
      end

      let(:selector) do
        {name: 'test2'}
      end

      let(:session) do
        authorized_client.start_session
      end

      let(:events) do
        subscriber.command_started_events('delete')
      end

      let(:collection) do
        authorized_collection.with(write_concern: {w: 2})
      end

      let!(:command) do
        Utils.get_command_event(authorized_client, 'delete') do |client|
          collection.delete_one(selector, session: session, write_concern: {w: 1},
            bypass_document_validation: true)
        end.command
      end

      it 'deletes one successfully with correct options sent to server' do
        expect(events.length).to eq(1)
        expect(command[:writeConcern]).to_not be_nil
        expect(command[:writeConcern][:w]).to eq(1)
        expect(command[:bypassDocumentValidation]).to eq(true)
      end
    end
  end

  describe '#delete_many' do

    before do
      authorized_collection.insert_many([{ field: 'test1' }, { field: 'test2' }])
    end

    context 'when a selector was provided' do

      let(:selector) do
        { field: 'test1' }
      end

      it 'deletes the matching documents in the collection' do
        expect(authorized_collection.delete_many(selector).deleted_count).to eq(1)
      end
    end

    context 'when no selector was provided' do

      it 'deletes all the documents in the collection' do
        expect(authorized_collection.delete_many.deleted_count).to eq(2)
      end
    end

    context 'when the deletes fail' do
      require_topology :single

      let(:result) do
        collection_invalid_write_concern.delete_many
      end

      it 'raises an OperationFailure' do
        expect {
          result
        }.to raise_exception(Mongo::Error::OperationFailure)
      end
    end

    context 'when a session is provided' do

      let(:session) do
        authorized_client.start_session
      end

      let(:operation) do
        authorized_collection.delete_many({}, session: session)
      end

      let(:failed_operation) do
        authorized_collection.delete_many({ '$._id' => 1}, session: session)
      end

      let(:client) do
        authorized_client
      end

      it_behaves_like 'an operation using a session'
      it_behaves_like 'a failed operation using a session'
    end

    context 'when unacknowledged writes are used with an explicit session' do

      let(:collection_with_unacknowledged_write_concern) do
        authorized_collection.with(write: { w: 0 })
      end

      let(:operation) do
        collection_with_unacknowledged_write_concern.delete_many({ '$._id' => 1}, session: session)
      end

      it_behaves_like 'an explicit session with an unacknowledged write'
    end

    context 'when unacknowledged writes are used with an implicit session' do

      let(:collection_with_unacknowledged_write_concern) do
        client.with(write: { w: 0 })[TEST_COLL]
      end

      let(:operation) do
        collection_with_unacknowledged_write_concern.delete_many({ '$._id' => 1 })
      end

      it_behaves_like 'an implicit session with an unacknowledged write'
    end

    context 'when a collation is specified' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        authorized_collection.delete_many(selector, options)
      end

      before do
        authorized_collection.insert_one(name: 'bang')
        authorized_collection.insert_one(name: 'bang')
      end

      let(:options) do
        { collation: { locale: 'en_US', strength: 2 } }
      end

      context 'when the server selected supports collations' do
        min_server_fcv '3.4'

        it 'applies the collation' do
          expect(result.written_count).to eq(2)
          expect(authorized_collection.find(name: 'bang').count).to eq(0)
        end

        context 'when unacknowledged writes is used' do

          let(:collection_with_unacknowledged_write_concern) do
            authorized_collection.with(write: { w: 0 })
          end

          let(:result) do
            collection_with_unacknowledged_write_concern.delete_many(selector, options)
          end

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

    context 'when a collation is not specified' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        authorized_collection.delete_many(selector)
      end

      before do
        authorized_collection.insert_one(name: 'bang')
        authorized_collection.insert_one(name: 'bang')
      end

      it 'does not apply the collation' do
        expect(result.written_count).to eq(0)
        expect(authorized_collection.find(name: 'bang').count).to eq(2)
      end
    end

    context 'when various options passed in' do
      # w: 2 requires a replica set
      require_topology :replica_set

      # https://jira.mongodb.org/browse/RUBY-2306
      min_server_fcv '3.6'

      before do
        collection.insert_many([{ name: 'test1' }, { name: 'test2' }, { name: 'test3'}])
      end

      let(:selector) do
        {name: 'test1'}
      end

      let(:session) do
        authorized_client.start_session
      end

      let(:events) do
        subscriber.command_started_events('delete')
      end

      let(:collection) do
        authorized_collection.with(write_concern: {w: 1})
      end

      let!(:command) do
        Utils.get_command_event(authorized_client, 'delete') do |client|
          collection.delete_many(selector, session: session, write_concern: {w: 2},
            bypass_document_validation: true)
        end.command
      end

      it 'deletes many successfully with correct options sent to server' do
        expect(events.length).to eq(1)
        expect(command[:writeConcern]).to_not be_nil
        expect(command[:writeConcern][:w]).to eq(2)
        expect(command[:bypassDocumentValidation]).to be(true)
      end
    end
  end

  describe '#parallel_scan' do
    max_server_version '4.0'
    require_topology :single, :replica_set

    let(:documents) do
      (1..200).map do |i|
        { name: "testing-scan-#{i}" }
      end
    end

    before do
      authorized_collection.insert_many(documents)
    end

    let(:cursors) do
      authorized_collection.parallel_scan(2)
    end

    it 'returns an array of cursors' do
      cursors.each do |cursor|
        expect(cursor.class).to be(Mongo::Cursor)
      end
    end

    it 'returns the correct number of documents' do
      expect(
        cursors.reduce(0) { |total, cursor| total + cursor.to_a.size }
      ).to eq(200)
    end

    context 'when a session is provided' do
      require_wired_tiger

      let(:cursors) do
        authorized_collection.parallel_scan(2, session: session)
      end

      let(:operation) do
        cursors.reduce(0) { |total, cursor| total + cursor.to_a.size }
      end

      let(:failed_operation) do
        authorized_collection.parallel_scan(-2, session: session)
      end

      let(:client) do
        authorized_client
      end

      it_behaves_like 'an operation using a session'
      it_behaves_like 'a failed operation using a session'
    end

    context 'when a session is not provided' do
      let(:collection) { client['test'] }

      let(:cursors) do
        collection.parallel_scan(2)
      end

      let(:operation) do
        cursors.reduce(0) { |total, cursor| total + cursor.to_a.size }
      end

      let(:failed_operation) do
        collection.parallel_scan(-2)
      end

      let(:command) do
        operation
        event = subscriber.started_events.find { |cmd| cmd.command_name == 'parallelCollectionScan' }
        expect(event).not_to be_nil
        event.command
      end

      it_behaves_like 'an operation not using a session'
      it_behaves_like 'a failed operation not using a session'
    end

    context 'when a session supporting causal consistency is used' do
      require_wired_tiger

      before do
        collection.drop
        collection.create
      end

      let(:cursors) do
        collection.parallel_scan(2, session: session)
      end

      let(:operation) do
        cursors.reduce(0) { |total, cursor| total + cursor.to_a.size }
      end

      let(:command) do
        operation
        event = subscriber.started_events.find { |cmd| cmd.command_name == 'parallelCollectionScan' }
        expect(event).not_to be_nil
        event.command
      end

      it_behaves_like 'an operation supporting causally consistent reads'
    end

    context 'when a read concern is provided' do
      require_wired_tiger
      min_server_fcv '3.2'

      let(:result) do
        authorized_collection.with(options).parallel_scan(2)
      end

      context 'when the read concern is valid' do

        let(:options) do
          { read_concern: { level: 'local' }}
        end

        it 'sends the read concern' do
          expect { result }.to_not raise_error
        end
      end

      context 'when the read concern is not valid' do

        let(:options) do
          { read_concern: { level: 'idontknow' }}
        end

        it 'raises an exception' do
          expect {
            result
          }.to raise_error(Mongo::Error::OperationFailure)
        end
      end
    end

    context 'when the collection has a read preference' do
      require_topology :single, :replica_set

      before do
        allow(collection.client.cluster).to receive(:single?).and_return(false)
      end

      let(:client) do
        authorized_client.with(server_selection_timeout: 0.2)
      end

      let(:collection) do
        client[authorized_collection.name,
               read: { :mode => :secondary, :tag_sets => [{ 'non' => 'existent' }] }]
      end

      let(:result) do
        collection.parallel_scan(2)
      end

      it 'uses that read preference' do
        expect {
          result
        }.to raise_exception(Mongo::Error::NoServerAvailable)
      end
    end

    context 'when a max time ms value is provided' do
      require_topology :single, :replica_set

      let(:result) do
        authorized_collection.parallel_scan(2, options)
      end

      context 'when the read concern is valid' do

        let(:options) do
          { max_time_ms: 5 }
        end

        it 'sends the max time ms value' do
          expect { result }.to_not raise_error
        end
      end

      context 'when the max time ms is not valid' do

        let(:options) do
          { max_time_ms: 0.1 }
        end

        it 'raises an exception' do
          expect {
            result
          }.to raise_error(Mongo::Error::OperationFailure)
        end
      end
    end
  end

  describe '#replace_one' do

    let(:selector) do
      { field: 'test1' }
    end

    context 'when a selector was provided' do

      before do
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test1' }])
      end

      let!(:response) do
        authorized_collection.replace_one(selector, { field: 'testing' })
      end

      let(:updated) do
        authorized_collection.find(field: 'testing').first
      end

      it 'updates the first matching document in the collection' do
        expect(response.modified_count).to eq(1)
      end

      it 'updates the documents in the collection' do
        expect(updated[:field]).to eq('testing')
      end
    end

    context 'when upsert is false' do

      let!(:response) do
        authorized_collection.replace_one(selector, { field: 'test1' }, upsert: false)
      end

      let(:updated) do
        authorized_collection.find(field: 'test1').to_a
      end

      it 'reports that no documents were written'  do
        expect(response.modified_count).to eq(0)
      end

      it 'does not insert the document' do
        expect(updated).to be_empty
      end
    end

    context 'when upsert is true' do

      let!(:response) do
        authorized_collection.replace_one(selector, { field: 'test1' }, upsert: true)
      end

      let(:updated) do
        authorized_collection.find(field: 'test1').first
      end

      it 'reports that a document was written' do
        expect(response.written_count).to eq(1)
      end

      it 'inserts the document' do
        expect(updated[:field]).to eq('test1')
      end
    end

    context 'when upsert is not specified' do

      let!(:response) do
        authorized_collection.replace_one(selector, { field: 'test1' })
      end

      let(:updated) do
        authorized_collection.find(field: 'test1').to_a
      end

      it 'reports that no documents were written' do
        expect(response.modified_count).to eq(0)
      end

      it 'does not insert the document' do
        expect(updated).to be_empty
      end
    end

    context 'when the replace has an invalid key' do

      context "when validate_update_replace is true" do

        config_override :validate_update_replace, true

        let(:result) do
          authorized_collection.replace_one(selector, { '$s' => 'test1' })
        end

        it 'raises an InvalidReplacementDocument error' do
          expect {
            result
          }.to raise_exception(Mongo::Error::InvalidReplacementDocument)
        end
      end

      context "when validate_update_replace is false" do

        config_override :validate_update_replace, false

        let(:result) do
          authorized_collection.replace_one(selector, { '$set' => { 'test1' => 1 } })
        end

        it 'does not raise an error' do
          expect {
            result
          }.to_not raise_exception
        end
      end
    end

    context 'when collection has a validator' do
      min_server_fcv '3.2'

      around(:each) do |spec|
        collection_with_validator.drop
        authorized_client[:validating,
                          :validator => { :a => { '$exists' => true } }].tap do |c|
          c.create
        end
        spec.run
        collection_with_validator.drop
      end

      before do
        collection_with_validator.insert_one({ a: 1 })
      end

      context 'when the document is valid' do

        let(:result) do
          collection_with_validator.replace_one({ a: 1 }, { a: 5 })
        end

        it 'replaces successfully' do
          expect(result.modified_count).to eq(1)
        end
      end

      context 'when the document is invalid' do

        context 'when bypass_document_validation is not set' do

          let(:result2) do
            collection_with_validator.replace_one({ a: 1 }, { x: 5 })
          end

          it 'raises OperationFailure' do
            expect {
              result2
            }.to raise_exception(Mongo::Error::OperationFailure)
          end
        end

        context 'when bypass_document_validation is true' do

          let(:result3) do
            collection_with_validator.replace_one(
              { a: 1 }, { x: 1 }, :bypass_document_validation => true)
          end

          it 'replaces successfully' do
            expect(result3.written_count).to eq(1)
          end
        end
      end
    end

    context 'when a collation is specified' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        authorized_collection.replace_one(selector, { name: 'doink' }, options)
      end

      before do
        authorized_collection.insert_one(name: 'bang')
      end

      let(:options) do
        { collation: { locale: 'en_US', strength: 2 } }
      end

      context 'when the server selected supports collations' do
        min_server_fcv '3.4'

        it 'applies the collation' do
          expect(result.written_count).to eq(1)
          expect(authorized_collection.find(name: 'doink').count).to eq(1)
        end

        context 'when unacknowledged writes is used' do

          let(:collection_with_unacknowledged_write_concern) do
            authorized_collection.with(write: { w: 0 })
          end

          let(:result) do
            collection_with_unacknowledged_write_concern.replace_one(selector, { name: 'doink' }, options)
          end

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

    context 'when a collation is not specified' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        authorized_collection.replace_one(selector, { name: 'doink' })
      end

      before do
        authorized_collection.insert_one(name: 'bang')
      end

      it 'does not apply the collation' do
        expect(result.written_count).to eq(0)
        expect(authorized_collection.find(name: 'bang').count).to eq(1)
      end
    end

    context 'when a session is provided' do

      let(:selector) do
        { name: 'BANG' }
      end

      before do
        authorized_collection.insert_one(name: 'bang')
      end

      let(:session) do
        authorized_client.start_session
      end

      let(:operation) do
        authorized_collection.replace_one(selector, { name: 'doink' }, session: session)
      end

      let(:failed_operation) do
        authorized_collection.replace_one({ '$._id' => 1 }, { name: 'doink' }, session: session)
      end

      let(:client) do
        authorized_client
      end

      it_behaves_like 'an operation using a session'
      it_behaves_like 'a failed operation using a session'
    end

    context 'when unacknowledged writes is used with an explicit session' do

      let(:collection_with_unacknowledged_write_concern) do
        authorized_collection.with(write: { w: 0 })
      end

      let(:operation) do
        collection_with_unacknowledged_write_concern.replace_one({ a: 1 }, { x: 5 }, session: session)
      end

      it_behaves_like 'an explicit session with an unacknowledged write'
    end

    context 'when unacknowledged writes is used with an implicit session' do

      let(:collection_with_unacknowledged_write_concern) do
        client.with(write: { w: 0 })[TEST_COLL]
      end

      let(:operation) do
        collection_with_unacknowledged_write_concern.replace_one({ a: 1 }, { x: 5 })
      end

      it_behaves_like 'an implicit session with an unacknowledged write'
    end

    context 'when various options passed in' do
      # w: 2 requires a replica set
      require_topology :replica_set

      # https://jira.mongodb.org/browse/RUBY-2306
      min_server_fcv '3.6'

      before do
        authorized_collection.insert_one({field: 'test1'})
      end

      let(:session) do
        authorized_client.start_session
      end

      let(:events) do
        subscriber.command_started_events('update')
      end

      let(:collection) do
        authorized_collection.with(write_concern: {w: 3})
      end

      let(:updated) do
        collection.find(field: 'test4').first
      end

      let!(:command) do
        Utils.get_command_event(authorized_client, 'update') do |client|
          collection.replace_one(selector, { field: 'test4'},
            session: session, :return_document => :after, write_concern: {w: 2},
            upsert: true, bypass_document_validation: true)
       end.command
      end

      it 'replaced one successfully with correct options sent to server' do
        expect(updated[:field]).to eq('test4')
        expect(events.length).to eq(1)
        expect(command[:writeConcern]).to_not be_nil
        expect(command[:writeConcern][:w]).to eq(2)
        expect(command[:bypassDocumentValidation]).to be(true)
        expect(command[:updates][0][:upsert]).to be(true)
      end
    end
  end

  describe '#update_many' do

    let(:selector) do
      { field: 'test' }
    end

    context 'when a selector was provided' do

      before do
        authorized_collection.insert_many([{ field: 'test' }, { field: 'test' }])
      end

      let!(:response) do
        authorized_collection.update_many(selector, '$set'=> { field: 'testing' })
      end

      let(:updated) do
        authorized_collection.find(field: 'testing').to_a.last
      end

      it 'returns the number updated' do
        expect(response.modified_count).to eq(2)
      end

      it 'updates the documents in the collection' do
        expect(updated[:field]).to eq('testing')
      end
    end

    context 'when upsert is false' do

      let(:response) do
        authorized_collection.update_many(selector, { '$set'=> { field: 'testing' } },
                                          upsert: false)
      end

      let(:updated) do
        authorized_collection.find.to_a
      end

      it 'reports that no documents were updated' do
        expect(response.modified_count).to eq(0)
      end

      it 'updates no documents in the collection' do
        expect(updated).to be_empty
      end
    end

    context 'when upsert is true' do

      let!(:response) do
        authorized_collection.update_many(selector, { '$set'=> { field: 'testing' } },
                                          upsert: true)
      end

      let(:updated) do
        authorized_collection.find.to_a.last
      end

      it 'reports that a document was written' do
        expect(response.written_count).to eq(1)
      end

      it 'inserts a document into the collection' do
        expect(updated[:field]).to eq('testing')
      end
    end

    context 'when upsert is not specified' do

      let(:response) do
        authorized_collection.update_many(selector, { '$set'=> { field: 'testing' } })
      end

      let(:updated) do
        authorized_collection.find.to_a
      end

      it 'reports that no documents were updated' do
        expect(response.modified_count).to eq(0)
      end

      it 'updates no documents in the collection' do
        expect(updated).to be_empty
      end
    end

    context 'when arrayFilters is provided' do

      let(:selector) do
        { '$or' => [{ _id: 0 }, { _id: 1 }]}
      end

      context 'when the server supports arrayFilters' do
        min_server_fcv '3.6'

        before do
          authorized_collection.insert_many([{
                                               _id: 0, x: [
                                                 { y: 1 },
                                                 { y: 2 },
                                                 { y: 3 }
                                               ]
                                             },
                                             {
                                               _id: 1,
                                               x: [
                                                 { y: 3 },
                                                 { y: 2 },
                                                 { y: 1 }
                                               ]
                                             }])
        end

        let(:result) do
          authorized_collection.update_many(selector,
                                            { '$set' => { 'x.$[i].y' => 5 } },
                                            options)
        end

        context 'when a Symbol key is used' do

          let(:options) do
            { array_filters: [{ 'i.y' => 3 }] }
          end

          it 'applies the arrayFilters' do
            expect(result.matched_count).to eq(2)
            expect(result.modified_count).to eq(2)

            docs = authorized_collection.find(selector, sort: { _id: 1 }).to_a
            expect(docs[0]['x']).to eq ([{ 'y' => 1 },  { 'y' => 2 }, { 'y' => 5 }])
            expect(docs[1]['x']).to eq ([{ 'y' => 5 },  { 'y' => 2 }, { 'y' => 1 }])
          end
        end

        context 'when a String key is used' do
          let(:options) do
            { 'array_filters' => [{ 'i.y' => 3 }] }
          end

          it 'applies the arrayFilters' do
            expect(result.matched_count).to eq(2)
            expect(result.modified_count).to eq(2)

            docs = authorized_collection.find({}, sort: { _id: 1 }).to_a
            expect(docs[0]['x']).to eq ([{ 'y' => 1 },  { 'y' => 2 }, { 'y' => 5 }])
            expect(docs[1]['x']).to eq ([{ 'y' => 5 },  { 'y' => 2 }, { 'y' => 1 }])
          end
        end
      end

      context 'when the server does not support arrayFilters' do
        max_server_version '3.4'

        let(:result) do
          authorized_collection.update_many(selector,
                                           { '$set' => { 'x.$[i].y' => 5 } },
                                           options)
        end

        context 'when a Symbol key is used' do

          let(:options) do
            { array_filters: [{ 'i.y' => 3 }] }
          end

          it 'raises an exception' do
            expect {
              result
            }.to raise_exception(Mongo::Error::UnsupportedArrayFilters)
          end
        end

        context 'when a String key is used' do

          let(:options) do
            { 'array_filters' => [{ 'i.y' => 3 }] }
          end

          it 'raises an exception' do
            expect {
              result
            }.to raise_exception(Mongo::Error::UnsupportedArrayFilters)
          end
        end
      end
    end

    context 'when the updates fail' do

      let(:result) do
        authorized_collection.update_many(selector, { '$s'=> { field: 'testing' } })
      end

      it 'raises an OperationFailure' do
        expect {
          result
        }.to raise_exception(Mongo::Error::OperationFailure)
      end
    end

    context 'when collection has a validator' do
      min_server_fcv '3.2'

      around(:each) do |spec|
        authorized_client[:validating,
                          :validator => { :a => { '$exists' => true } }].tap do |c|
          c.create
        end
        spec.run
        collection_with_validator.drop
      end

      before do
        collection_with_validator.insert_many([{ a: 1 }, { a: 2 }])
      end

      context 'when the document is valid' do

        let(:result) do
          collection_with_validator.update_many(
            { :a => { '$gt' => 0 } }, '$inc' => { :a => 1 } )
        end

        it 'updates successfully' do
          expect(result.modified_count).to eq(2)
        end
      end

      context 'when the document is invalid' do

        context 'when bypass_document_validation is not set' do

          let(:result2) do
            collection_with_validator.update_many(
              { :a => { '$gt' => 0 } }, '$unset' => { :a => '' })
          end

          it 'raises OperationFailure' do
            expect {
              result2
            }.to raise_exception(Mongo::Error::OperationFailure)
          end
        end

        context 'when bypass_document_validation is true' do

          let(:result3) do
            collection_with_validator.update_many(
              { :a => { '$gt' => 0 } }, { '$unset' => { :a => '' } },
              :bypass_document_validation => true)
          end

          it 'updates successfully' do
            expect(result3.written_count).to eq(2)
          end
        end
      end
    end

    context 'when a collation is specified' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        authorized_collection.update_many(selector, { '$set' => { other: 'doink' } }, options)
      end

      before do
        authorized_collection.insert_one(name: 'bang')
        authorized_collection.insert_one(name: 'baNG')
      end

      let(:options) do
        { collation: { locale: 'en_US', strength: 2 } }
      end

      context 'when the server selected supports collations' do
        min_server_fcv '3.4'

        it 'applies the collation' do
          expect(result.written_count).to eq(2)
          expect(authorized_collection.find(other: 'doink').count).to eq(2)
        end

        context 'when unacknowledged writes is used' do

          let(:collection_with_unacknowledged_write_concern) do
            authorized_collection.with(write: { w: 0 })
          end

          let(:result) do
            collection_with_unacknowledged_write_concern.update_many(selector, { '$set' => { other: 'doink' } }, options)
          end

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

    context 'when collation is not specified' do

      let(:selector) do
        {name: 'BANG'}
      end

      let(:result) do
        authorized_collection.update_many(selector, { '$set' => {other: 'doink'} })
      end

      before do
        authorized_collection.insert_one(name: 'bang')
        authorized_collection.insert_one(name: 'baNG')
      end

      it 'does not apply the collation' do
        expect(result.written_count).to eq(0)
      end
    end

    context 'when a session is provided' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:operation) do
        authorized_collection.update_many(selector, { '$set' => {other: 'doink'} }, session: session)
      end

      before do
        authorized_collection.insert_one(name: 'bang')
        authorized_collection.insert_one(name: 'baNG')
      end

      let(:session) do
        authorized_client.start_session
      end

      let(:failed_operation) do
        authorized_collection.update_many({ '$._id' => 1 }, { '$set' => {other: 'doink'} }, session: session)
      end

      let(:client) do
        authorized_client
      end

      it_behaves_like 'an operation using a session'
      it_behaves_like 'a failed operation using a session'
    end

    context 'when unacknowledged writes is used with an explicit session' do

      let(:collection_with_unacknowledged_write_concern) do
        authorized_collection.with(write: { w: 0 })
      end

      let(:operation) do
        collection_with_unacknowledged_write_concern.update_many({a: 1}, { '$set' => {x: 1} }, session: session)
      end

      it_behaves_like 'an explicit session with an unacknowledged write'
    end

    context 'when unacknowledged writes is used with an implicit session' do

      let(:collection_with_unacknowledged_write_concern) do
        client.with(write: { w: 0 })[TEST_COLL]
      end

      let(:operation) do
        collection_with_unacknowledged_write_concern.update_many({a: 1}, {'$set' => {x: 1}})
      end

      it_behaves_like 'an implicit session with an unacknowledged write'
    end

    context 'when various options passed in' do
      # w: 2 requires a replica set
      require_topology :replica_set

      # https://jira.mongodb.org/browse/RUBY-2306
      min_server_fcv '3.6'

      before do
        collection.insert_many([{ field: 'test' }, { field: 'test2' }], session: session)
      end

      let(:session) do
        authorized_client.start_session
      end

      let(:collection) do
        authorized_collection.with(write_concern: {w: 1})
      end

      let(:events) do
        subscriber.command_started_events('update')
      end

      let!(:command) do
        Utils.get_command_event(authorized_client, 'update') do |client|
          collection.update_many(selector, {'$set'=> { field: 'testing' }}, session: session,
            write_concern: {w: 2}, bypass_document_validation: true, upsert: true)
        end.command
      end

      it 'updates many successfully with correct options sent to server' do
        expect(events.length).to eq(1)
        expect(collection.options[:write_concern]).to eq(w: 1)
        expect(command[:writeConcern][:w]).to eq(2)
        expect(command[:bypassDocumentValidation]).to be(true)
        expect(command[:updates][0][:upsert]).to be(true)
      end
    end
  end

  describe '#update_one' do

    let(:selector) do
      { field: 'test1' }
    end

    context 'when a selector was provided' do

      before do
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test1' }])
      end

      let!(:response) do
        authorized_collection.update_one(selector, '$set'=> { field: 'testing' })
      end

      let(:updated) do
        authorized_collection.find(field: 'testing').first
      end

      it 'updates the first matching document in the collection' do
        expect(response.modified_count).to eq(1)
      end

      it 'updates the documents in the collection' do
        expect(updated[:field]).to eq('testing')
      end
    end

    context 'when upsert is false' do

      let(:response) do
        authorized_collection.update_one(selector, { '$set'=> { field: 'testing' } },
                                         upsert: false)
      end

      let(:updated) do
        authorized_collection.find.to_a
      end

      it 'reports that no documents were updated' do
        expect(response.modified_count).to eq(0)
      end

      it 'updates no documents in the collection' do
        expect(updated).to be_empty
      end
    end

    context 'when upsert is true' do

      let!(:response) do
        authorized_collection.update_one(selector, { '$set'=> { field: 'testing' } },
                                         upsert: true)
      end

      let(:updated) do
        authorized_collection.find.first
      end

      it 'reports that a document was written' do
        expect(response.written_count).to eq(1)
      end

      it 'inserts a document into the collection' do
        expect(updated[:field]).to eq('testing')
      end
    end

    context 'when upsert is not specified' do

      let(:response) do
        authorized_collection.update_one(selector, { '$set'=> { field: 'testing' } })
      end

      let(:updated) do
        authorized_collection.find.to_a
      end

      it 'reports that no documents were updated' do
        expect(response.modified_count).to eq(0)
      end

      it 'updates no documents in the collection' do
        expect(updated).to be_empty
      end
    end

    context 'when the update fails' do

      let(:result) do
        authorized_collection.update_one(selector, { '$s'=> { field: 'testing' } })
      end

      it 'raises an OperationFailure' do
        expect {
          result
        }.to raise_exception(Mongo::Error::OperationFailure)
      end
    end

    context 'when collection has a validator' do
      min_server_fcv '3.2'

      around(:each) do |spec|
        authorized_client[:validating,
                          :validator => { :a => { '$exists' => true } }].tap do |c|
          c.create
        end
        spec.run
        collection_with_validator.drop
      end

      before do
        collection_with_validator.insert_one({ a: 1 })
      end

      context 'when the document is valid' do

        let(:result) do
          collection_with_validator.update_one(
            { :a => { '$gt' => 0 } }, '$inc' => { :a => 1 } )
        end

        it 'updates successfully' do
          expect(result.modified_count).to eq(1)
        end
      end

      context 'when the document is invalid' do

        context 'when bypass_document_validation is not set' do

          let(:result2) do
            collection_with_validator.update_one(
              { :a => { '$gt' => 0 } }, '$unset' => { :a => '' })
          end

          it 'raises OperationFailure' do
            expect {
              result2
            }.to raise_exception(Mongo::Error::OperationFailure)
          end
        end

        context 'when bypass_document_validation is true' do

          let(:result3) do
            collection_with_validator.update_one(
              { :a => { '$gt' => 0 } }, { '$unset' => { :a => '' } },
              :bypass_document_validation => true)
          end

          it 'updates successfully' do
            expect(result3.written_count).to eq(1)
          end
        end
      end
    end

    context 'when there is a collation specified' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        authorized_collection.update_one(selector, { '$set' => { other: 'doink' } }, options)
      end

      before do
        authorized_collection.insert_one(name: 'bang')
      end

      let(:options) do
        { collation: { locale: 'en_US', strength: 2 } }
      end

      context 'when the server selected supports collations' do
        min_server_fcv '3.4'

        it 'applies the collation' do
          expect(result.written_count).to eq(1)
          expect(authorized_collection.find(other: 'doink').count).to eq(1)
        end

        context 'when unacknowledged writes is used' do

          let(:collection_with_unacknowledged_write_concern) do
            authorized_collection.with(write: { w: 0 })
          end

          let(:result) do
            collection_with_unacknowledged_write_concern.update_one(selector, { '$set' => { other: 'doink' } }, options)
          end

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

    context 'when a collation is not specified' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        authorized_collection.update_one(selector, { '$set' => { other: 'doink' } })
      end

      before do
        authorized_collection.insert_one(name: 'bang')
      end

      it 'does not apply the collation' do
        expect(result.written_count).to eq(0)
      end
    end


    context 'when arrayFilters is provided' do

      let(:selector) do
        { _id: 0}
      end

      context 'when the server supports arrayFilters' do
        min_server_fcv '3.6'

        before do
          authorized_collection.insert_one(_id: 0, x: [{ y: 1 }, { y: 2 }, {y: 3 }])
        end

        let(:result) do
          authorized_collection.update_one(selector,
                                           { '$set' => { 'x.$[i].y' => 5 } },
                                           options)
        end

        context 'when a Symbol key is used' do

          let(:options) do
            { array_filters: [{ 'i.y' => 3 }] }
          end

          it 'applies the arrayFilters' do
            expect(result.matched_count).to eq(1)
            expect(result.modified_count).to eq(1)
            expect(authorized_collection.find(selector).first['x'].last['y']).to eq(5)
          end
        end

        context 'when a String key is used' do

          let(:options) do
            { 'array_filters' => [{ 'i.y' => 3 }] }
          end

          it 'applies the arrayFilters' do
            expect(result.matched_count).to eq(1)
            expect(result.modified_count).to eq(1)
            expect(authorized_collection.find(selector).first['x'].last['y']).to eq(5)
          end
        end
      end

      context 'when the server does not support arrayFilters' do
        max_server_version '3.4'

        let(:result) do
          authorized_collection.update_one(selector,
                                           { '$set' => { 'x.$[i].y' => 5 } },
                                           options)
        end

        context 'when a Symbol key is used' do

          let(:options) do
            { array_filters: [{ 'i.y' => 3 }] }
          end

          it 'raises an exception' do
            expect {
              result
            }.to raise_exception(Mongo::Error::UnsupportedArrayFilters)
          end
        end

        context 'when a String key is used' do

          let(:options) do
            { 'array_filters' => [{ 'i.y' => 3 }] }
          end

          it 'raises an exception' do
            expect {
              result
            }.to raise_exception(Mongo::Error::UnsupportedArrayFilters)
          end
        end
      end
    end

    context 'when the documents are sent with OP_MSG' do
      min_server_fcv '3.6'

      let(:documents) do
        [{ '_id' => 1, 'name' => '1'*16777191 }, { '_id' => 'y' }]
      end

      before do
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test1' }])
        client[TEST_COLL].update_one({ a: 1 }, {'$set' => { 'name' => '1'*16777149 }})
      end

      let(:update_events) do
        subscriber.started_events.select { |e| e.command_name == 'update' }
      end

      it 'sends the documents in one OP_MSG' do
        expect(update_events.size).to eq(1)
      end
    end

    context 'when a session is provided' do

      before do
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test1' }])
      end

      let(:session) do
        authorized_client.start_session
      end

      let(:operation) do
        authorized_collection.update_one({ field: 'test' }, { '$set'=> { field: 'testing' } }, session: session)
      end

      let(:failed_operation) do
        authorized_collection.update_one({ '$._id' => 1 }, { '$set'=> { field: 'testing' } }, session: session)
      end

      let(:client) do
        authorized_client
      end

      it_behaves_like 'an operation using a session'
      it_behaves_like 'a failed operation using a session'
    end

    context 'when unacknowledged writes is used with an explicit session' do

      let(:collection_with_unacknowledged_write_concern) do
        authorized_collection.with(write: { w: 0 })
      end

      let(:operation) do
        collection_with_unacknowledged_write_concern.update_one({ a: 1 }, { '$set' => { x: 1 } }, session: session)
      end

      it_behaves_like 'an explicit session with an unacknowledged write'
    end

    context 'when unacknowledged writes is used with an implicit session' do

      let(:collection_with_unacknowledged_write_concern) do
        client.with(write: { w: 0 })[TEST_COLL]
      end

      let(:operation) do
        collection_with_unacknowledged_write_concern.update_one({ a: 1 }, { '$set' => { x: 1 }})
      end

      it_behaves_like 'an implicit session with an unacknowledged write'
    end

    context 'when various options passed in' do
      # w: 2 requires a replica set
      require_topology :replica_set

      # https://jira.mongodb.org/browse/RUBY-2306
      min_server_fcv '3.6'

      before do
        collection.insert_many([{ field: 'test1' }, { field: 'test2' }], session: session)
      end

      let(:session) do
        authorized_client.start_session
      end

      let(:collection) do
        authorized_collection.with(write_concern: {w: 1})
      end

      let(:events) do
        subscriber.command_started_events('update')
      end

      let!(:command) do
        Utils.get_command_event(authorized_client, 'update') do |client|
          collection.update_one(selector, { '$set'=> { field: 'testing' } }, session: session,
            write_concern: {w: 2}, bypass_document_validation: true, :return_document => :after,
            upsert: true)
        end.command
      end

      it 'updates one successfully with correct options sent to server' do
        expect(events.length).to eq(1)
        expect(command[:writeConcern]).to_not be_nil
        expect(command[:writeConcern][:w]).to eq(2)
        expect(collection.options[:write_concern]).to eq(w:1)
        expect(command[:bypassDocumentValidation]).to be(true)
        expect(command[:updates][0][:upsert]).to be(true)
      end
    end
  end

  describe '#find_one_and_delete' do

    before do
      authorized_collection.insert_many([{ field: 'test1' }])
    end

    let(:selector) do
      { field: 'test1' }
    end

    context 'when a matching document is found' do

      context 'when a session is provided' do

        let(:operation) do
          authorized_collection.find_one_and_delete(selector, session: session)
        end

        let(:failed_operation) do
          authorized_collection.find_one_and_delete({ '$._id' => 1 }, session: session)
        end

        let(:session) do
          authorized_client.start_session
        end

        let(:client) do
          authorized_client
        end

        it_behaves_like 'an operation using a session'
        it_behaves_like 'a failed operation using a session'
      end

      context 'when no options are provided' do

        let!(:document) do
          authorized_collection.find_one_and_delete(selector)
        end

        it 'deletes the document from the database' do
          expect(authorized_collection.find.to_a).to be_empty
        end

        it 'returns the document' do
          expect(document['field']).to eq('test1')
        end
      end

      context 'when a projection is provided' do

        let!(:document) do
          authorized_collection.find_one_and_delete(selector, projection: { _id: 1 })
        end

        it 'deletes the document from the database' do
          expect(authorized_collection.find.to_a).to be_empty
        end

        it 'returns the document with limited fields' do
          expect(document['field']).to be_nil
          expect(document['_id']).to_not be_nil
        end
      end

      context 'when a sort is provided' do

        let!(:document) do
          authorized_collection.find_one_and_delete(selector, sort: { field: 1 })
        end

        it 'deletes the document from the database' do
          expect(authorized_collection.find.to_a).to be_empty
        end

        it 'returns the document with limited fields' do
          expect(document['field']).to eq('test1')
        end
      end

      context 'when max_time_ms is provided' do

        it 'includes the max_time_ms value in the command' do
          expect {
            authorized_collection.find_one_and_delete(selector, max_time_ms: 0.1)
          }.to raise_error(Mongo::Error::OperationFailure)
        end
      end
    end

    context 'when no matching document is found' do

      let(:selector) do
        { field: 'test5' }
      end

      let!(:document) do
        authorized_collection.find_one_and_delete(selector)
      end

      it 'returns nil' do
        expect(document).to be_nil
      end
    end

    context 'when the operation fails' do

      let(:result) do
        authorized_collection.find_one_and_delete(selector, max_time_ms: 0.1)
      end

      it 'raises an OperationFailure' do
        expect {
          result
        }.to raise_exception(Mongo::Error::OperationFailure)
      end
    end

    context 'when write_concern is provided' do
      min_server_fcv '3.2'
      require_topology :single

      it 'uses the write concern' do
        expect {
          authorized_collection.find_one_and_delete(selector,
                                                    write_concern: { w: 2 })
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end

    context 'when the collection has a write concern' do
      min_server_fcv '3.2'
      require_topology :single

      let(:collection) do
        authorized_collection.with(write: { w: 2 })
      end

      it 'uses the write concern' do
        expect {
          collection.find_one_and_delete(selector,
                                         write_concern: { w: 2 })
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end

    context 'when collation is specified' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        authorized_collection.find_one_and_delete(selector, options)
      end

      before do
        authorized_collection.insert_one(name: 'bang')
      end

      let(:options) do
        { collation: { locale: 'en_US', strength: 2 } }
      end

      context 'when the server selected supports collations' do
        min_server_fcv '3.4'

        it 'applies the collation' do
          expect(result['name']).to eq('bang')
          expect(authorized_collection.find(name: 'bang').count).to eq(0)
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

    context 'when collation is not specified' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        authorized_collection.find_one_and_delete(selector)
      end

      before do
        authorized_collection.insert_one(name: 'bang')
      end

      it 'does not apply the collation' do
        expect(result).to be_nil
      end
    end

    context 'when various options passed in' do
      # w: 2 requires a replica set
      require_topology :replica_set

      # https://jira.mongodb.org/browse/RUBY-2306
      min_server_fcv '3.6'

      before do
        authorized_collection.delete_many
        authorized_collection.insert_many([{ name: 'test1' }, { name: 'test2' }])
      end

      let(:collection) do
        authorized_collection.with(write_concern: {w: 2})
      end

      let(:session) do
        authorized_client.start_session
      end

      let!(:command) do
        Utils.get_command_event(authorized_client, 'findAndModify') do |client|
          collection.find_one_and_delete(selector, session: session, write_concern: {w: 2},
            bypass_document_validation: true, max_time_ms: 300)
        end.command
      end

      let(:events) do
        subscriber.command_started_events('findAndModify')
      end

      it 'finds and deletes successfully with correct options sent to server' do
        expect(events.length).to eq(1)
        expect(command[:writeConcern]).to_not be_nil
        expect(command[:writeConcern][:w]).to eq(2)
        expect(command[:bypassDocumentValidation]).to eq(true)
        expect(command[:maxTimeMS]).to eq(300)
      end
    end
  end

  describe '#find_one_and_update' do

    let(:selector) do
      { field: 'test1' }
    end

    before do
      authorized_collection.insert_many([{ field: 'test1' }])
    end

    context 'when a matching document is found' do

      context 'when no options are provided' do

        let(:document) do
          authorized_collection.find_one_and_update(selector, { '$set' => { field: 'testing' }})
        end

        it 'returns the original document' do
          expect(document['field']).to eq('test1')
        end
      end

      context 'when a session is provided' do

        let(:operation) do
          authorized_collection.find_one_and_update(selector, { '$set' => { field: 'testing' }}, session: session)
        end

        let(:failed_operation) do
          authorized_collection.find_one_and_update({ '$._id' => 1 }, { '$set' => { field: 'testing' }}, session: session)
        end

        let(:session) do
          authorized_client.start_session
        end

        let(:client) do
          authorized_client
        end

        it_behaves_like 'an operation using a session'
        it_behaves_like 'a failed operation using a session'
      end

      context 'when no options are provided' do

        let(:document) do
          authorized_collection.find_one_and_update(selector, { '$set' => { field: 'testing' }})
        end

        it 'returns the original document' do
          expect(document['field']).to eq('test1')
        end
      end

      context 'when return_document options are provided' do

        context 'when return_document is :after' do

          let(:document) do
            authorized_collection.find_one_and_update(selector, { '$set' => { field: 'testing' }}, :return_document => :after)
          end

          it 'returns the new document' do
            expect(document['field']).to eq('testing')
          end
        end

        context 'when return_document is :before' do

          let(:document) do
            authorized_collection.find_one_and_update(selector, { '$set' => { field: 'testing' }}, :return_document => :before)
          end

          it 'returns the original document' do
            expect(document['field']).to eq('test1')
          end
        end
      end

      context 'when a projection is provided' do

        let(:document) do
          authorized_collection.find_one_and_update(selector, { '$set' => { field: 'testing' }}, projection: { _id: 1 })
        end

        it 'returns the document with limited fields' do
          expect(document['field']).to be_nil
          expect(document['_id']).to_not be_nil
        end
      end

      context 'when a sort is provided' do

        let(:document) do
          authorized_collection.find_one_and_update(selector, { '$set' => { field: 'testing' }}, sort: { field: 1 })
        end

        it 'returns the original document' do
          expect(document['field']).to eq('test1')
        end
      end
    end

    context 'when max_time_ms is provided' do

      it 'includes the max_time_ms value in the command' do
        expect {
          authorized_collection.find_one_and_update(selector, { '$set' => { field: 'testing' }}, max_time_ms: 0.1)
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end

    context 'when no matching document is found' do

      let(:selector) do
        { field: 'test5' }
      end

      let(:document) do
        authorized_collection.find_one_and_update(selector, { '$set' => { field: 'testing' }})
      end

      it 'returns nil' do
        expect(document).to be_nil
      end
    end

    context 'when no matching document is found' do

      context 'when no upsert options are provided' do

        let(:selector) do
          { field: 'test5' }
        end

        let(:document) do
          authorized_collection.find_one_and_update(selector, { '$set' => { field: 'testing' }})
        end

        it 'returns nil' do
          expect(document).to be_nil
        end
      end

      context 'when upsert options are provided' do

        let(:selector) do
          { field: 'test5' }
        end

        let(:document) do
          authorized_collection.find_one_and_update(selector, { '$set' => { field: 'testing' }}, :upsert => true, :return_document => :after)
        end

        it 'returns the new document' do
          expect(document['field']).to eq('testing')
        end
      end
    end

    context 'when the operation fails' do

      let(:result) do
        authorized_collection.find_one_and_update(selector, { '$set' => { field: 'testing' }}, max_time_ms: 0.1)
      end

      it 'raises an OperationFailure' do
        expect {
          result
        }.to raise_exception(Mongo::Error::OperationFailure)
      end
    end

    context 'when collection has a validator' do
      min_server_fcv '3.2'

      around(:each) do |spec|
        authorized_client[:validating].drop
        authorized_client[:validating,
                          :validator => { :a => { '$exists' => true } }].tap do |c|
          c.create
        end
        spec.run
        collection_with_validator.drop
      end

      before do
        collection_with_validator.insert_one({ a: 1 })
      end

      context 'when the document is valid' do

        let(:result) do
          collection_with_validator.find_one_and_update(
            { a: 1 }, { '$inc' => { :a => 1 } }, :return_document => :after)
        end

        it 'updates successfully' do
          expect(result['a']).to eq(2)
        end
      end

      context 'when the document is invalid' do

        context 'when bypass_document_validation is not set' do

          let(:result2) do
            collection_with_validator.find_one_and_update(
              { a: 1 }, { '$unset' => { :a => '' } }, :return_document => :after)
          end

          it 'raises OperationFailure' do
            expect {
              result2
            }.to raise_exception(Mongo::Error::OperationFailure)
          end
        end

        context 'when bypass_document_validation is true' do

          let(:result3) do
            collection_with_validator.find_one_and_update(
              { a: 1 }, { '$unset' => { :a => '' } },
              :bypass_document_validation => true,
              :return_document => :after)
          end

          it 'updates successfully' do
            expect(result3['a']).to be_nil
          end
        end
      end
    end

    context 'when write_concern is provided' do
      min_server_fcv '3.2'
      require_topology :single

      it 'uses the write concern' do
        expect {
          authorized_collection.find_one_and_update(selector,
                                                    { '$set' => { field: 'testing' }},
                                                    write_concern: { w: 2 })
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end

    context 'when the collection has a write concern' do
      min_server_fcv '3.2'
      require_topology :single

      let(:collection) do
        authorized_collection.with(write: { w: 2 })
      end

      it 'uses the write concern' do
        expect {
          collection.find_one_and_update(selector,
                                         { '$set' => { field: 'testing' }},
                                         write_concern: { w: 2 })
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end

    context 'when a collation is specified' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        authorized_collection.find_one_and_update(selector,
                                                  { '$set' => { other: 'doink' } },
                                                  options)
      end

      before do
        authorized_collection.insert_one(name: 'bang')
      end

      let(:options) do
        { collation: { locale: 'en_US', strength: 2 } }
      end

      context 'when the server selected supports collations' do
        min_server_fcv '3.4'

        it 'applies the collation' do
          expect(result['name']).to eq('bang')
          expect(authorized_collection.find({ name: 'bang' }, limit: -1).first['other']).to eq('doink')
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

    context 'when there is no collation specified' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        authorized_collection.find_one_and_update(selector, { '$set' => { other: 'doink' } })
      end

      before do
        authorized_collection.insert_one(name: 'bang')
      end

      it 'does not apply the collation' do
        expect(result).to be_nil
      end
    end

    context 'when arrayFilters is provided' do

      let(:selector) do
        { _id: 0 }
      end

      context 'when the server supports arrayFilters' do
        min_server_fcv '3.6'

        before do
          authorized_collection.insert_one(_id: 0, x: [{ y: 1 }, { y: 2 }, { y: 3 }])
        end

        let(:result) do
          authorized_collection.find_one_and_update(selector,
                                                    { '$set' => { 'x.$[i].y' => 5 } },
                                                    options)
        end

        context 'when a Symbol key is used' do

          let(:options) do
            { array_filters: [{ 'i.y' => 3 }] }
          end


          it 'applies the arrayFilters' do
            expect(result['x']).to eq([{ 'y' => 1 }, { 'y' => 2 }, { 'y' => 3 }])
            expect(authorized_collection.find(selector).first['x'].last['y']).to eq(5)
          end
        end

        context 'when a String key is used' do

          let(:options) do
            { 'array_filters' => [{ 'i.y' => 3 }] }
          end

          it 'applies the arrayFilters' do
            expect(result['x']).to eq([{ 'y' => 1 }, { 'y' => 2 }, { 'y' => 3 }])
            expect(authorized_collection.find(selector).first['x'].last['y']).to eq(5)
          end
        end
      end

      context 'when the server selected does not support arrayFilters' do
        max_server_version '3.4'

        let(:result) do
          authorized_collection.find_one_and_update(selector,
                                                    { '$set' => { 'x.$[i].y' => 5 } },
                                                    options)
        end

        context 'when a Symbol key is used' do

          let(:options) do
            { array_filters: [{ 'i.y' => 3 }] }
          end

          it 'raises an exception' do
            expect {
              result
            }.to raise_exception(Mongo::Error::UnsupportedArrayFilters)
          end
        end

        context 'when a String key is used' do

          let(:options) do
            { 'array_filters' => [{ 'i.y' => 3 }] }
          end

          it 'raises an exception' do
            expect {
              result
            }.to raise_exception(Mongo::Error::UnsupportedArrayFilters)
          end
        end
      end
    end

    context 'when various options passed in' do
      # w: 2 requires a replica set
      require_topology :replica_set

      # https://jira.mongodb.org/browse/RUBY-2306
      min_server_fcv '3.6'

      let(:session) do
        authorized_client.start_session
      end

      let(:events) do
        subscriber.command_started_events('findAndModify')
      end

      let(:collection) do
        authorized_collection.with(write_concern: {w: 2})
      end

      let(:selector) do
        {field: 'test1'}
      end

      before do
        collection.insert_one({field: 'test1'}, session: session)
      end

      let!(:command) do
        Utils.get_command_event(authorized_client, 'findAndModify') do |client|
          collection.find_one_and_update(selector, { '$set' => {field: 'testing'}},
            :return_document => :after, write_concern: {w: 1}, upsert: true,
            bypass_document_validation: true, max_time_ms: 100, session: session)
        end.command
      end

      it 'find and updates successfully with correct options sent to server' do
        expect(events.length).to eq(1)
        expect(command[:writeConcern]).to_not be_nil
        expect(command[:writeConcern][:w]).to eq(1)
        expect(command[:upsert]).to eq(true)
        expect(command[:bypassDocumentValidation]).to be(true)
        expect(command[:maxTimeMS]).to eq(100)
      end
    end
  end

  describe '#find_one_and_replace' do

    before do
      authorized_collection.insert_many([{ field: 'test1', other: 'sth' }])
    end

    let(:selector) do
      { field: 'test1' }
    end

    context 'when a matching document is found' do

      context 'when no options are provided' do

        let(:document) do
          authorized_collection.find_one_and_replace(selector, { field: 'testing' })
        end

        it 'returns the original document' do
          expect(document['field']).to eq('test1')
        end
      end

      context 'when a session is provided' do

        let(:operation) do
          authorized_collection.find_one_and_replace(selector, { field: 'testing' }, session: session)
        end

        let(:failed_operation) do
          authorized_collection.find_one_and_replace({ '$._id' => 1}, { field: 'testing' }, session: session)
        end

        let(:session) do
          authorized_client.start_session
        end

        let(:client) do
          authorized_client
        end

        it_behaves_like 'an operation using a session'
        it_behaves_like 'a failed operation using a session'
      end

      context 'when return_document options are provided' do

        context 'when return_document is :after' do

          let(:document) do
            authorized_collection.find_one_and_replace(selector, { field: 'testing' }, :return_document => :after)
          end

          it 'returns the new document' do
            expect(document['field']).to eq('testing')
          end
        end

        context 'when return_document is :before' do

          let(:document) do
            authorized_collection.find_one_and_replace(selector, { field: 'testing' }, :return_document => :before)
          end

          it 'returns the original document' do
            expect(document['field']).to eq('test1')
          end
        end
      end

      context 'when a projection is provided' do

        let(:document) do
          authorized_collection.find_one_and_replace(selector, { field: 'testing' }, projection: { _id: 1 })
        end

        it 'returns the document with limited fields' do
          expect(document['field']).to be_nil
          expect(document['_id']).to_not be_nil
        end
      end

      context 'when a sort is provided' do

        let(:document) do
          authorized_collection.find_one_and_replace(selector, { field: 'testing' }, :sort => { field: 1 })
        end

        it 'returns the original document' do
          expect(document['field']).to eq('test1')
        end
      end
    end

    context 'when no matching document is found' do

      context 'when no upsert options are provided' do

        let(:selector) do
          { field: 'test5' }
        end

        let(:document) do
          authorized_collection.find_one_and_replace(selector, { field: 'testing' })
        end

        it 'returns nil' do
          expect(document).to be_nil
        end
      end

      context 'when upsert options are provided' do

        let(:selector) do
          { field: 'test5' }
        end

        let(:document) do
          authorized_collection.find_one_and_replace(selector, { field: 'testing' }, :upsert => true, :return_document => :after)
        end

        it 'returns the new document' do
          expect(document['field']).to eq('testing')
        end
      end
    end

    context 'when max_time_ms is provided' do

      it 'includes the max_time_ms value in the command' do
        expect {
          authorized_collection.find_one_and_replace(selector, { field: 'testing' }, max_time_ms: 0.1)
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end

    context 'when the operation fails' do

      let(:result) do
        authorized_collection.find_one_and_replace(selector, { field: 'testing' }, max_time_ms: 0.1)
      end

      it 'raises an OperationFailure' do
        expect {
          result
        }.to raise_exception(Mongo::Error::OperationFailure)
      end
    end

    context 'when collection has a validator' do
      min_server_fcv '3.2'

      around(:each) do |spec|
        authorized_client[:validating].drop
        authorized_client[:validating,
                          :validator => { :a => { '$exists' => true } }].tap do |c|
          c.create
        end
        spec.run
        collection_with_validator.drop
      end

      before do
        collection_with_validator.insert_one({ a: 1 })
      end

      context 'when the document is valid' do

        let(:result) do
          collection_with_validator.find_one_and_replace(
            { a: 1 }, { a: 5 }, :return_document => :after)
        end

        it 'replaces successfully when document is valid' do
          expect(result[:a]).to eq(5)
        end
      end

      context 'when the document is invalid' do

        context 'when bypass_document_validation is not set' do

          let(:result2) do
            collection_with_validator.find_one_and_replace(
              { a: 1 }, { x: 5 }, :return_document => :after)
          end

          it 'raises OperationFailure' do
            expect {
              result2
            }.to raise_exception(Mongo::Error::OperationFailure)
          end
        end

        context 'when bypass_document_validation is true' do

          let(:result3) do
            collection_with_validator.find_one_and_replace(
              { a: 1 }, { x: 1 }, :bypass_document_validation => true,
              :return_document => :after)
          end

          it 'replaces successfully' do
            expect(result3[:x]).to eq(1)
            expect(result3[:a]).to be_nil
          end
        end
      end
    end

    context 'when write_concern is provided' do
      min_server_fcv '3.2'
      require_topology :single

      it 'uses the write concern' do
        expect {
          authorized_collection.find_one_and_replace(selector,
                                                     { field: 'testing' },
                                                     write_concern: { w: 2 })
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end

    context 'when the collection has a write concern' do
      min_server_fcv '3.2'
      require_topology :single

      let(:collection) do
        authorized_collection.with(write: { w: 2 })
      end

      it 'uses the write concern' do
        expect {
          collection.find_one_and_replace(selector,
                                          { field: 'testing' },
                                          write_concern: { w: 2 })
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end

    context 'when collation is provided' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        authorized_collection.find_one_and_replace(selector,
                                                   { name: 'doink' },
                                                   options)
      end

      before do
        authorized_collection.insert_one(name: 'bang')
      end

      let(:options) do
        { collation: { locale: 'en_US', strength: 2 } }
      end

      context 'when the server selected supports collations' do
        min_server_fcv '3.4'

        it 'applies the collation' do
          expect(result['name']).to eq('bang')
          expect(authorized_collection.find(name: 'doink').count).to eq(1)
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

    context 'when collation is not specified' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        authorized_collection.find_one_and_replace(selector, { name: 'doink' })
      end

      before do
        authorized_collection.insert_one(name: 'bang')
      end

      it 'does not apply the collation' do
        expect(result).to be_nil
      end
    end

    context 'when various options passed in' do
      # https://jira.mongodb.org/browse/RUBY-2306
      min_server_fcv '3.6'

      before do
        authorized_collection.insert_one({field: 'test1'})
      end

      let(:session) do
        authorized_client.start_session
      end

      let(:events) do
        subscriber.command_started_events('findAndModify')
      end

      let(:collection) do
        authorized_collection.with(write_concern: { w: 2 })
      end

      let!(:command) do
        Utils.get_command_event(authorized_client, 'findAndModify') do |client|
          collection.find_one_and_replace(selector, { '$set' => {field: 'test5'}},
            :return_document => :after, write_concern: {w: 1}, session: session,
            upsert: true, bypass_document_validation: false, max_time_ms: 200)
        end.command
      end

      it 'find and replaces successfully with correct options sent to server' do
        expect(events.length).to eq(1)
        expect(command[:writeConcern]).to_not be_nil
        expect(command[:writeConcern][:w]).to eq(1)
        expect(command[:upsert]).to be(true)
        expect(command[:bypassDocumentValidation]).to be false
        expect(command[:maxTimeMS]).to eq(200)
      end
    end
  end

  context 'when unacknowledged writes is used on find_one_and_update' do

    let(:selector) do
      { name: 'BANG' }
    end

    let(:collection_with_unacknowledged_write_concern) do
      authorized_collection.with(write: { w: 0 })
    end

    let(:result) do
      collection_with_unacknowledged_write_concern.find_one_and_update(selector,
        { '$set' => { field: 'testing' }},
        write_concern: { w: 0 })
    end

    it 'does not raise an exception' do
      expect(result).to be_nil
    end
  end

  context "when creating collection with view_on and pipeline" do
    before do
      authorized_client["my_view"].drop
      authorized_collection.insert_one({ bar: "here!" })
      authorized_client["my_view",
        view_on: authorized_collection.name,
        pipeline: [ { :'$project' => { "baz": "$bar" } } ]
      ].create
    end

    it "the view has a document" do
      expect(authorized_client["my_view"].find.to_a.length).to eq(1)
    end

    it "applies the pipeline" do
      expect(authorized_client["my_view"].find.first).to have_key("baz")
      expect(authorized_client["my_view"].find.first["baz"]).to eq("here!")
    end
  end
end
