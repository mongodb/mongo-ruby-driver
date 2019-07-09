require 'spec_helper'

describe Mongo::BulkWrite do

  before do
    authorized_collection.drop
  end

  let(:collection_invalid_write_concern) do
    authorized_collection.client.with(write: INVALID_WRITE_CONCERN)[authorized_collection.name]
  end

  let(:collation) do
    { locale: 'en_US', strength: 2 }
  end

  let(:array_filters) do
    [{ 'i.y' => 3}]
  end

  let(:collection) do
    authorized_collection
  end

  let(:client) do
    authorized_client
  end

  shared_examples_for 'bulk write with write concern yielding operation failure' do
    require_topology :single

    it 'raises an OperationFailure' do
      expect {
        bulk_write_invalid_write_concern.execute
      }.to raise_error(Mongo::Error::OperationFailure)
    end
  end

  describe '#execute' do
    shared_examples_for 'an executable bulk write' do

      context 'when providing a bad operation' do

        let(:requests) do
          [{ not_an_operation: { _id: 0 }}]
        end

        it 'raises an exception' do
          expect {
            bulk_write.execute
          }.to raise_error(Mongo::Error::InvalidBulkOperationType)
        end
      end

      context 'when the operations do not need to be split' do

        context 'when a write error occurs' do

          let(:requests) do
            [
              { insert_one: { _id: 0 }},
              { insert_one: { _id: 1 }},
              { insert_one: { _id: 0 }},
              { insert_one: { _id: 1 }}
            ]
          end

          let(:error) do
            begin
              bulk_write.execute
            rescue => e
              e
            end
          end

          it 'raises an exception' do
            expect {
              bulk_write.execute
            }.to raise_error(Mongo::Error::BulkWriteError)
          end

          it 'sets the document index on the error' do
            expect(error.result['writeErrors'].first['index']).to eq(2)
          end

          context 'when a session is provided' do

            let(:extra_options) do
              { session: session }
            end

            let(:client) do
              authorized_client
            end

            let(:failed_operation) do
              bulk_write.execute
            end

            it_behaves_like 'a failed operation using a session'
          end
        end

        context 'when provided a single insert one' do

          let(:requests) do
            [{ insert_one: { _id: 0 }}]
          end

          let(:result) do
            bulk_write.execute
          end

          it 'inserts the document' do
            expect(result.inserted_count).to eq(1)
            expect(authorized_collection.find(_id: 0).count).to eq(1)
          end

          it 'only inserts that document' do
            result
            expect(authorized_collection.find.first['_id']).to eq(0)
          end

          context 'when a session is provided' do

            let(:operation) do
              result
            end

            let(:extra_options) do
              { session: session }
            end

            let(:client) do
              authorized_client
            end

            it_behaves_like 'an operation using a session'
          end

          context 'when there is a write concern error' do

            it_behaves_like 'bulk write with write concern yielding operation failure'

            context 'when a session is provided' do

              let(:extra_options) do
                {session: session}
              end

              let(:client) do
                collection_invalid_write_concern.client
              end

              let(:failed_operation) do
                bulk_write_invalid_write_concern.execute
              end

              it_behaves_like 'a failed operation using a session'
            end
          end
        end

        context 'when provided multiple insert ones' do

          let(:requests) do
            [
              { insert_one: { _id: 0 }},
              { insert_one: { _id: 1 }},
              { insert_one: { _id: 2 }}
            ]
          end

          let(:result) do
            bulk_write.execute
          end

          it 'inserts the documents' do
            expect(result.inserted_count).to eq(3)
            expect(authorized_collection.find(_id: { '$in'=> [ 0, 1, 2 ]}).count).to eq(3)
          end

          context 'when there is a write failure' do

            let(:requests) do
              [{ insert_one: { _id: 1 }}, { insert_one: { _id: 1 }}]
            end

            it 'raises a BulkWriteError' do
              expect {
                bulk_write.execute
              }.to raise_error(Mongo::Error::BulkWriteError)
            end
          end

          context 'when there is a write concern error' do

            it_behaves_like 'bulk write with write concern yielding operation failure'

            context 'when a session is provided' do

              let(:extra_options) do
                {session: session}
              end

              let(:client) do
                collection_invalid_write_concern.client
              end

              let(:failed_operation) do
                bulk_write_invalid_write_concern.execute
              end

              it_behaves_like 'a failed operation using a session'
            end
          end
        end

        context 'when provided a single delete one' do

          let(:requests) do
            [{ delete_one: { filter: { _id: 0 }}}]
          end

          let(:result) do
            bulk_write.execute
          end

          before do
            authorized_collection.insert_one({ _id: 0 })
          end

          it 'deletes the document' do
            expect(result.deleted_count).to eq(1)
            expect(authorized_collection.find(_id: 0).count).to eq(0)
          end

          context 'when a session is provided' do

            let(:operation) do
              result
            end

            let(:client) do
              authorized_client
            end

            let(:extra_options) do
              { session: session }
            end

            it_behaves_like 'an operation using a session'
          end

          context 'when there is a write concern error' do

            it_behaves_like 'bulk write with write concern yielding operation failure'

            context 'when a session is provided' do

              let(:extra_options) do
                {session: session}
              end

              let(:client) do
                collection_invalid_write_concern.client
              end

              let(:failed_operation) do
                bulk_write_invalid_write_concern.execute
              end

              it_behaves_like 'a failed operation using a session'
            end

            context 'when the write has a collation specified' do

              before do
                authorized_collection.insert_one(name: 'bang')
              end

              let(:requests) do
                [{ delete_one: { filter: { name: 'BANG' }, collation: collation } }]
              end

              context 'when the server selected supports collations' do
                min_server_fcv '3.4'

                let!(:result) do
                  bulk_write.execute
                end

                it 'applies the collation' do
                  expect(authorized_collection.find(name: 'bang').count).to eq(0)
                end

                it 'reports the deleted count' do
                  expect(result.deleted_count).to eq(1)
                end
              end

              context 'when the server selected does not support collations' do
                max_server_version '3.2'

                it 'raises an exception' do
                  expect {
                    bulk_write.execute
                  }.to raise_exception(Mongo::Error::UnsupportedCollation)
                end

                context 'when a String key is used' do

                  let(:requests) do
                    [{ delete_one: { filter: { name: 'BANG' }, 'collation' => collation } }]
                  end

                  it 'raises an exception' do
                    expect {
                      bulk_write.execute
                    }.to raise_exception(Mongo::Error::UnsupportedCollation)
                  end
                end
              end
            end

            context 'when a collation is not specified' do

              before do
                authorized_collection.insert_one(name: 'bang')
              end

              let(:requests) do
                [{ delete_one: { filter: { name: 'BANG' }}}]
              end

              let!(:result) do
                bulk_write.execute
              end

              it 'does not apply the collation' do
                expect(authorized_collection.find(name: 'bang').count).to eq(1)
              end

              it 'reports the deleted count' do
                expect(result.deleted_count).to eq(0)
              end
            end
          end

          context 'when bulk executing update_one' do

            context 'when the write has specified arrayFilters' do

              before do
                authorized_collection.insert_one(_id: 1, x: [{ y: 1 }, { y: 2 }, { y: 3 }])
              end

              let(:requests) do
                [{
                   update_one: {
                     filter: { _id: 1 },
                     update: { '$set' => { 'x.$[i].y' => 5 } },
                     array_filters: array_filters,
                   }
                 }]
              end

              context 'when the server selected supports arrayFilters' do
                min_server_fcv '3.6'

                let!(:result) do
                  bulk_write.execute
                end

                it 'applies the arrayFilters' do
                  expect(result.matched_count).to eq(1)
                  expect(result.modified_count).to eq(1)
                  expect(authorized_collection.find(_id: 1).first['x'].last['y']).to eq(5)
                end
              end

              context 'when the server selected does not support arrayFilters' do
                max_server_version '3.4'

                it 'raises an exception' do
                  expect {
                    bulk_write.execute
                  }.to raise_exception(Mongo::Error::UnsupportedArrayFilters)
                end
              end
            end
          end

          context 'when bulk executing update_many' do

            context 'when the write has specified arrayFilters' do

              before do
                authorized_collection.insert_many([{
                                                     _id: 1, x: [
                                                       { y: 1 },
                                                       { y: 2 },
                                                       { y: 3 }
                                                     ]
                                                   },
                                                   {
                                                     _id: 2,
                                                     x: [
                                                       { y: 3 },
                                                       { y: 2 },
                                                       { y: 1 }
                                                     ]
                                                   }])
              end

              let(:selector) do
                { '$or' => [{ _id: 1 }, { _id: 2 }]}
              end

              let(:requests) do
                [{
                   update_many: {
                     filter: { '$or' => [{ _id: 1 }, { _id: 2 }]},
                     update: { '$set' => { 'x.$[i].y' => 5 } },
                     array_filters: array_filters,
                   }
                 }]
              end

              context 'when the server selected supports arrayFilters' do
                min_server_fcv '3.6'

                let!(:result) do
                  bulk_write.execute
                end

                it 'applies the arrayFilters' do
                  expect(result.matched_count).to eq(2)
                  expect(result.modified_count).to eq(2)

                  docs = authorized_collection.find(selector, sort: { _id: 1 }).to_a
                  expect(docs[0]['x']).to eq ([{ 'y' => 1 },  { 'y' => 2 }, { 'y' => 5}])
                  expect(docs[1]['x']).to eq ([{ 'y' => 5 },  { 'y' => 2 }, { 'y' => 1}])
                end
              end

              context 'when the server selected does not support arrayFilters' do
                max_server_version '3.4'

                it 'raises an exception' do
                  expect {
                    bulk_write.execute
                  }.to raise_exception(Mongo::Error::UnsupportedArrayFilters)
                end
              end
            end
          end

          context 'when multiple documents match delete selector' do

            before do
              authorized_collection.insert_many([{ a: 1 }, { a: 1 }])
            end

            let(:requests) do
              [{ delete_one: { filter: { a: 1 }}}]
            end

            it 'reports n_removed correctly' do
              expect(bulk_write.execute.deleted_count).to eq(1)
            end

            it 'deletes only matching documents' do
              bulk_write.execute
              expect(authorized_collection.find(a: 1).count).to eq(1)
            end
          end
        end

        context 'when provided multiple delete ones' do

          let(:requests) do
            [
              { delete_one: { filter: { _id: 0 }}},
              { delete_one: { filter: { _id: 1 }}},
              { delete_one: { filter: { _id: 2 }}}
            ]
          end

          let(:result) do
            bulk_write.execute
          end

          before do
            authorized_collection.insert_many([
              { _id: 0 }, { _id: 1 }, { _id: 2 }
            ])
          end

          it 'deletes the documents' do
            expect(result.deleted_count).to eq(3)
            expect(authorized_collection.find(_id: { '$in'=> [ 0, 1, 2 ]}).count).to eq(0)
          end

          context 'when a session is provided' do

            let(:operation) do
              result
            end

            let(:client) do
              authorized_client
            end

            let(:extra_options) do
              { session: session }
            end

            it_behaves_like 'an operation using a session'
          end

          context 'when there is a write concern error' do

            it_behaves_like 'bulk write with write concern yielding operation failure'

            context 'when a session is provided' do

              let(:extra_options) do
                {session: session}
              end

              let(:client) do
                collection_invalid_write_concern.client
              end

              let(:failed_operation) do
                bulk_write_invalid_write_concern.execute
              end

              it_behaves_like 'a failed operation using a session'
            end
          end

          context 'when the write has a collation specified' do

            before do
              authorized_collection.insert_one(name: 'bang')
              authorized_collection.insert_one(name: 'doink')
            end

            let(:requests) do
              [{ delete_one: { filter: { name: 'BANG' }, collation: collation }},
               { delete_one: { filter: { name: 'DOINK' }, collation: collation }}]
            end

            context 'when the server selected supports collations' do
              min_server_fcv '3.4'

              let!(:result) do
                bulk_write.execute
              end

              it 'applies the collation' do
                expect(authorized_collection.find(name: { '$in' => ['bang', 'doink']}).count).to eq(0)
              end

              it 'reports the deleted count' do
                expect(result.deleted_count).to eq(2)
              end
            end

            context 'when the server selected does not support collations' do
              max_server_version '3.2'

              it 'raises an exception' do
                expect {
                  bulk_write.execute
                }.to raise_exception(Mongo::Error::UnsupportedCollation)
              end

              context 'when a String key is used' do

                let(:requests) do
                  [{ delete_one: { filter: { name: 'BANG' }, 'collation' => collation }},
                   { delete_one: { filter: { name: 'DOINK' }, 'collation' => collation }}]
                end

                it 'raises an exception' do
                  expect {
                    bulk_write.execute
                  }.to raise_exception(Mongo::Error::UnsupportedCollation)
                end
              end
            end
          end

          context 'when the write does not have a collation specified' do

            before do
              authorized_collection.insert_one(name: 'bang')
              authorized_collection.insert_one(name: 'doink')
            end

            let(:requests) do
              [{ delete_one: { filter: { name: 'BANG' }}},
               { delete_one: { filter: { name: 'DOINK' }}}]
            end

            let!(:result) do
              bulk_write.execute
            end

            it 'does not apply the collation' do
              expect(authorized_collection.find(name: { '$in' => ['bang', 'doink']}).count).to eq(2)
            end

            it 'reports the deleted count' do
              expect(result.deleted_count).to eq(0)
            end
          end
        end

        context 'when provided a single delete many' do

          let(:requests) do
            [{ delete_many: { filter: { _id: 0 }}}]
          end

          let(:result) do
            bulk_write.execute
          end

          before do
            authorized_collection.insert_one({ _id: 0 })
          end

          it 'deletes the documents' do
            expect(result.deleted_count).to eq(1)
            expect(authorized_collection.find(_id: 0).count).to eq(0)
          end

          context 'when a session is provided' do

            let(:operation) do
              result
            end

            let(:client) do
              authorized_client
            end

            let(:extra_options) do
              { session: session }
            end

            it_behaves_like 'an operation using a session'
          end

          context 'when there is a write concern error' do

            it_behaves_like 'bulk write with write concern yielding operation failure'

            context 'when a session is provided' do

              let(:extra_options) do
                {session: session}
              end

              let(:client) do
                collection_invalid_write_concern.client
              end

              let(:failed_operation) do
                bulk_write_invalid_write_concern.execute
              end

              it_behaves_like 'a failed operation using a session'
            end
          end

          context 'when the write has a collation specified' do

            before do
              authorized_collection.insert_one(name: 'bang')
              authorized_collection.insert_one(name: 'bang')
            end

            let(:requests) do
              [{ delete_many: { filter: { name: 'BANG' }, collation: collation }}]
            end

            context 'when the server selected supports collations' do
              min_server_fcv '3.4'

              let!(:result) do
                bulk_write.execute
              end

              it 'applies the collation' do
                expect(authorized_collection.find(name: 'bang').count).to eq(0)
              end

              it 'reports the deleted count' do
                expect(result.deleted_count).to eq(2)
              end
            end

            context 'when the server selected does not support collations' do
              max_server_version '3.2'

              it 'raises an exception' do
                expect {
                  bulk_write.execute
                }.to raise_exception(Mongo::Error::UnsupportedCollation)
              end

              context 'when a String key is used' do

                let(:requests) do
                  [{ delete_many: { filter: { name: 'BANG' }, 'collation' => collation }}]
                end

                it 'raises an exception' do
                  expect {
                    bulk_write.execute
                  }.to raise_exception(Mongo::Error::UnsupportedCollation)
                end
              end
            end
          end

          context 'when a collation is not specified' do

            before do
              authorized_collection.insert_one(name: 'bang')
              authorized_collection.insert_one(name: 'bang')
            end

            let(:requests) do
              [{ delete_many: { filter: { name: 'BANG' }}}]
            end

            let!(:result) do
              bulk_write.execute
            end

            it 'does not apply the collation' do
              expect(authorized_collection.find(name: 'bang').count).to eq(2)
            end

            it 'reports the deleted count' do
              expect(result.deleted_count).to eq(0)
            end
          end
        end

        context 'when provided multiple delete many ops' do

          let(:requests) do
            [
              { delete_many: { filter: { _id: 0 }}},
              { delete_many: { filter: { _id: 1 }}},
              { delete_many: { filter: { _id: 2 }}}
            ]
          end

          let(:result) do
            bulk_write.execute
          end

          before do
            authorized_collection.insert_many([
              { _id: 0 }, { _id: 1 }, { _id: 2 }
            ])
          end

          it 'deletes the documents' do
            expect(result.deleted_count).to eq(3)
            expect(authorized_collection.find(_id: { '$in'=> [ 0, 1, 2 ]}).count).to eq(0)
          end

          context 'when a session is provided' do

            let(:operation) do
              result
            end

            let(:client) do
              authorized_client
            end

            let(:extra_options) do
              { session: session }
            end

            it_behaves_like 'an operation using a session'
          end

          context 'when there is a write concern error' do

            it_behaves_like 'bulk write with write concern yielding operation failure'

            context 'when a session is provided' do

              let(:operation) do
                result
              end

              let(:client) do
                authorized_client
              end

              let(:extra_options) do
                {session: session}
              end

              it_behaves_like 'an operation using a session'
            end
          end

          context 'when the write has a collation specified' do

            before do
              authorized_collection.insert_one(name: 'bang')
              authorized_collection.insert_one(name: 'bang')
              authorized_collection.insert_one(name: 'doink')
            end

            let(:requests) do
              [{ delete_many: { filter: { name: 'BANG' },  collation: collation }},
               { delete_many: { filter: { name: 'DOINK' },  collation: collation }}]
            end

            context 'when the server selected supports collations' do
              min_server_fcv '3.4'

              let!(:result) do
                bulk_write.execute
              end

              it 'applies the collation' do
                expect(authorized_collection.find(name: { '$in' => ['bang', 'doink'] }).count).to eq(0)
              end

              it 'reports the deleted count' do
                expect(result.deleted_count).to eq(3)
              end
            end

            context 'when the server selected does not support collations' do
              max_server_version '3.2'

              it 'raises an exception' do
                expect {
                  bulk_write.execute
                }.to raise_exception(Mongo::Error::UnsupportedCollation)
              end

              context 'when a String key is used' do

                let(:requests) do
                  [{ delete_many: { filter: { name: 'BANG' },  'collation' => collation }},
                   { delete_many: { filter: { name: 'DOINK' },  'collation' => collation }}]
                end

                it 'raises an exception' do
                  expect {
                    bulk_write.execute
                  }.to raise_exception(Mongo::Error::UnsupportedCollation)
                end
              end
            end
          end

          context 'when a collation is not specified' do

            before do
              authorized_collection.insert_one(name: 'bang')
              authorized_collection.insert_one(name: 'bang')
              authorized_collection.insert_one(name: 'doink')
            end

            let(:requests) do
              [{ delete_many: { filter: { name: 'BANG' }}},
               { delete_many: { filter: { name: 'DOINK' }}}]
            end

            let!(:result) do
              bulk_write.execute
            end

            it 'does not apply the collation' do
              expect(authorized_collection.find(name: { '$in' => ['bang', 'doink'] }).count).to eq(3)
            end

            it 'reports the deleted count' do
              expect(result.deleted_count).to eq(0)
            end
          end
        end

        context 'when providing a single replace one' do

          let(:requests) do
            [{ replace_one: { filter: { _id: 0 }, replacement: { name: 'test' }}}]
          end

          let(:result) do
            bulk_write.execute
          end

          before do
            authorized_collection.insert_one({ _id: 0 })
          end

          it 'replaces the document' do
            expect(result.modified_count).to eq(1)
            expect(authorized_collection.find(_id: 0).first[:name]).to eq('test')
          end

          context 'when a session is provided' do

            let(:operation) do
              result
            end

            let(:client) do
              authorized_client
            end

            let(:extra_options) do
              { session: session }
            end

            it_behaves_like 'an operation using a session'
          end

          context 'when there is a write concern error' do

            it_behaves_like 'bulk write with write concern yielding operation failure'

            context 'when a session is provided' do

              let(:extra_options) do
                {session: session}
              end

              let(:client) do
                collection_invalid_write_concern.client
              end

              let(:failed_operation) do
                bulk_write_invalid_write_concern.execute
              end

              it_behaves_like 'a failed operation using a session'
            end
          end

          context 'when the write has a collation specified' do

            before do
              authorized_collection.insert_one(name: 'bang')
            end

            let(:requests) do
              [{ replace_one: { filter: { name: 'BANG' },
                                replacement: { other: 'pong' },
                                collation: collation }}]
            end

            context 'when the server selected supports collations' do
              min_server_fcv '3.4'

              let!(:result) do
                bulk_write.execute
              end

              it 'applies the collation' do
                expect(authorized_collection.find(other: 'pong').count).to eq(1)
              end

              it 'reports the upserted id' do
                expect(result.upserted_ids).to eq([])
              end

              it 'reports the upserted count' do
                expect(result.upserted_count).to eq(0)
              end

              it 'reports the modified count' do
                expect(result.modified_count).to eq(1)
              end

              it 'reports the matched count' do
                expect(result.matched_count).to eq(1)
              end
            end

            context 'when the server selected does not support collations' do
              max_server_version '3.2'

              it 'raises an exception' do
                expect {
                  bulk_write.execute
                }.to raise_exception(Mongo::Error::UnsupportedCollation)
              end

              context 'when a String key is used' do

                let(:requests) do
                  [{ replace_one: { filter: { name: 'BANG' },
                                    replacement: { other: 'pong' },
                                    'collation' => collation }}]
                end

                it 'raises an exception' do
                  expect {
                    bulk_write.execute
                  }.to raise_exception(Mongo::Error::UnsupportedCollation)
                end
              end
            end
          end

          context 'when the write does not have a collation specified' do

            before do
              authorized_collection.insert_one(name: 'bang')
            end

            let(:requests) do
              [{ replace_one: { filter: { name: 'BANG' }, replacement: { other: 'pong' }}}]
            end

            let!(:result) do
              bulk_write.execute
            end

            it 'does not apply the collation' do
              expect(authorized_collection.find(other: 'pong').count).to eq(0)
            end

            it 'reports the upserted id' do
              expect(result.upserted_ids).to eq([])
            end

            it 'reports the upserted count' do
              expect(result.upserted_count).to eq(0)
            end

            it 'reports the modified count' do
              expect(result.modified_count).to eq(0)
            end

            it 'reports the matched count' do
              expect(result.matched_count).to eq(0)
            end
          end
        end

        context 'when providing a single update one' do

          context 'when upsert is false' do

            let(:requests) do
              [{ update_one: { filter: { _id: 0 }, update: { "$set" => { name: 'test' }}}}]
            end

            let(:result) do
              bulk_write.execute
            end

            before do
              authorized_collection.insert_one({ _id: 0 })
            end

            it 'updates the document' do
              result
              expect(authorized_collection.find(_id: 0).first[:name]).to eq('test')
            end

            it 'reports the upserted id' do
              expect(result.upserted_ids).to eq([])
            end

            it 'reports the upserted count' do
              expect(result.upserted_count).to eq(0)
            end

            it 'reports the modified count' do
              expect(result.modified_count).to eq(1)
            end

            it 'reports the matched count' do
              expect(result.matched_count).to eq(1)
            end

            context 'when a session is provided' do

              let(:operation) do
                result
              end

              let(:client) do
                authorized_client
              end

              let(:extra_options) do
                { session: session }
              end

              it_behaves_like 'an operation using a session'
            end

            context 'when documents match but are not modified' do

              before do
                authorized_collection.insert_one({ a: 0 })
              end

              let(:requests) do
                [{ update_one: { filter: { a: 0 }, update: { "$set" => { a: 0 }}}}]
              end

              it 'reports the upserted id' do
                expect(result.upserted_ids).to eq([])
              end

              it 'reports the upserted count' do
                expect(result.upserted_count).to eq(0)
              end

              it 'reports the modified count' do
                expect(result.modified_count).to eq(0)
              end

              it 'reports the matched count' do
                expect(result.matched_count).to eq(1)
              end
            end

            context 'when the number of updates exceeds the max batch size' do

              let(:batch_size) do
                11
              end

              before do
                allow(client.cluster.next_primary).to receive(:max_write_batch_size).and_return(batch_size - 1)
              end

              let(:requests) do
                batch_size.times.collect do |i|
                  { update_one: { filter: { a: i }, update: { "$set" => { a: i, b: 3 }}, upsert: true }}
                end
              end

              it 'updates the documents and reports the correct number of upserted ids' do
                expect(result.upserted_ids.size).to eq(batch_size)
                expect(authorized_collection.find(b: 3).count).to eq(batch_size)
              end
            end

            context 'when there is a write concern error' do

              it_behaves_like 'bulk write with write concern yielding operation failure'

              context 'when a session is provided' do

                let(:extra_options) do
                  {session: session}
                end

                let(:client) do
                  collection_invalid_write_concern.client
                end

                let(:failed_operation) do
                  bulk_write_invalid_write_concern.execute
                end

                it_behaves_like 'a failed operation using a session'
              end
            end
          end

          context 'when upsert is true' do

            let(:requests) do
              [{ update_one: { filter: { _id: 0 }, update: { "$set" => { name: 'test' } }, upsert: true }}]
            end

            let(:result) do
              bulk_write.execute
            end

            it 'updates the document' do
              result
              expect(authorized_collection.find(_id: 0).first[:name]).to eq('test')
            end

            it 'reports the upserted count' do
              expect(result.upserted_count).to eq(1)
            end

            it 'reports the modified_count count' do
              expect(result.modified_count).to eq(0)
            end

            it 'reports the matched count' do
              expect(result.matched_count).to eq(0)
            end

            it 'reports the upserted id' do
              expect(result.upserted_ids).to eq([0])
            end

            context 'when there is a write concern error' do

              it_behaves_like 'bulk write with write concern yielding operation failure'
            end

            context 'when write_concern is specified as an option' do

              # In a multi-sharded cluster, the write seems to go to a
              # different shard from the read
              require_no_multi_shard

              let(:extra_options) do
                { write_concern: { w: 0 } }
              end

              let(:result) do
                bulk_write.execute
              end

              it 'updates the document' do
                result
                expect(authorized_collection.find(_id: 0).first[:name]).to eq('test')
              end

              it 'does not report the upserted count' do
                expect(result.upserted_count).to eq(0)
              end

              it 'does not report the modified_count count' do
                expect(result.modified_count).to eq(0)
              end

              it 'does not report the matched count' do
                expect(result.matched_count).to eq(0)
              end

              it 'does not report the upserted id' do
                expect(result.upserted_ids).to eq([])
              end
            end
          end

          context 'when the write has a collation specified' do

            before do
              authorized_collection.insert_one(name: 'bang')
            end

            let(:requests) do
              [{ update_one: { filter: { name: 'BANG' },
                               update: { "$set" => { name: 'pong' }},
                               collation: collation }}]
            end

            context 'when the server selected supports collations' do
              min_server_fcv '3.4'

              let!(:result) do
                bulk_write.execute
              end

              it 'applies the collation' do
                expect(authorized_collection.find(name: 'pong').count).to eq(1)
              end

              it 'reports the upserted id' do
                expect(result.upserted_ids).to eq([])
              end

              it 'reports the upserted count' do
                expect(result.upserted_count).to eq(0)
              end

              it 'reports the modified count' do
                expect(result.modified_count).to eq(1)
              end

              it 'reports the matched count' do
                expect(result.matched_count).to eq(1)
              end
            end

            context 'when the server selected does not support collations' do
              max_server_version '3.2'

              it 'raises an exception' do
                expect {
                  bulk_write.execute
                }.to raise_exception(Mongo::Error::UnsupportedCollation)
              end

              context 'when a String key is used' do

                let(:requests) do
                  [{ update_one: { filter: { name: 'BANG' },
                                   update: { "$set" => { name: 'pong' }},
                                   'collation' => collation }}]
                end

                it 'raises an exception' do
                  expect {
                    bulk_write.execute
                  }.to raise_exception(Mongo::Error::UnsupportedCollation)
                end
              end
            end
          end

          context 'when the write does not have a collation specified' do

            before do
              authorized_collection.insert_one(name: 'bang')
            end

            let(:requests) do
              [{ update_one: { filter: { name: 'BANG' }, update: { "$set" => { name: 'pong' }}}}]
            end

            let!(:result) do
              bulk_write.execute
            end

            it 'does not apply the collation' do
              expect(authorized_collection.find(name: 'pong').count).to eq(0)
            end

            it 'reports the upserted id' do
              expect(result.upserted_ids).to eq([])
            end

            it 'reports the upserted count' do
              expect(result.upserted_count).to eq(0)
            end

            it 'reports the modified count' do
              expect(result.modified_count).to eq(0)
            end

            it 'reports the matched count' do
              expect(result.matched_count).to eq(0)
            end
          end
        end

        context 'when providing multiple update ones' do

          context 'when the write has a collation specified' do

            before do
              authorized_collection.insert_one(name: 'bang')
              authorized_collection.insert_one(name: 'doink')
            end

            let(:requests) do
              [{ update_one: { filter: { name: 'BANG' },
                               update: { "$set" => { name: 'pong' }},
                               collation: collation }},
               { update_one: { filter: { name: 'DOINK' },
                               update: { "$set" => { name: 'pong' }},
                               collation: collation }}]
            end

            context 'when the server selected supports collations' do
              min_server_fcv '3.4'

              let!(:result) do
                bulk_write.execute
              end

              it 'applies the collation' do
                expect(authorized_collection.find(name: 'pong').count).to eq(2)
              end

              it 'reports the upserted id' do
                expect(result.upserted_ids).to eq([])
              end

              it 'reports the upserted count' do
                expect(result.upserted_count).to eq(0)
              end

              it 'reports the modified count' do
                expect(result.modified_count).to eq(2)
              end

              it 'reports the matched count' do
                expect(result.matched_count).to eq(2)
              end
            end

            context 'when the server selected does not support collations' do
              max_server_version '3.2'

              it 'raises an exception' do
                expect {
                  bulk_write.execute
                }.to raise_exception(Mongo::Error::UnsupportedCollation)
              end

              context 'when a String key is used' do

                let(:requests) do
                  [{ update_one: { filter: { name: 'BANG' },
                                   update: { "$set" => { name: 'pong' }},
                                   'collation' => collation }},
                   { update_one: { filter: { name: 'DOINK' },
                                   update: { "$set" => { name: 'pong' }},
                                   'collation' => collation }}]
                end

                it 'raises an exception' do
                  expect {
                    bulk_write.execute
                  }.to raise_exception(Mongo::Error::UnsupportedCollation)
                end
              end
            end
          end

          context 'when the write does not have a collation specified' do

            before do
              authorized_collection.insert_one(name: 'bang')
              authorized_collection.insert_one(name: 'doink')
            end

            let(:requests) do
              [{ update_one: { filter: { name: 'BANG' }, update: { "$set" => { name: 'pong' }}}},
               { update_one: { filter: { name: 'DOINK' }, update: { "$set" => { name: 'pong' }}}}]
            end

            let!(:result) do
              bulk_write.execute
            end

            it 'does not apply the collation' do
              expect(authorized_collection.find(name: 'pong').count).to eq(0)
            end

            it 'reports the upserted id' do
              expect(result.upserted_ids).to eq([])
            end

            it 'reports the upserted count' do
              expect(result.upserted_count).to eq(0)
            end

            it 'reports the modified count' do
              expect(result.modified_count).to eq(0)
            end

            it 'reports the matched count' do
              expect(result.matched_count).to eq(0)
            end
          end

          context 'when upsert is false' do

            let(:requests) do
              [{ update_one: { filter: { _id: 0 }, update: { "$set" => { name: 'test' }}}},
               { update_one: { filter: { _id: 1 }, update: { "$set" => { name: 'test' }}}}]
            end

            let(:result) do
              bulk_write.execute
            end

            before do
              authorized_collection.insert_many([{ _id: 0 }, { _id: 1 }])
            end

            it 'updates the document' do
              result
              expect(authorized_collection.find(name: 'test').count).to eq(2)
            end

            it 'reports the upserted id' do
              expect(result.upserted_ids).to eq([])
            end

            it 'reports the upserted count' do
              expect(result.upserted_count).to eq(0)
            end

            it 'reports the modified count' do
              expect(result.modified_count).to eq(2)
            end

            it 'reports the matched count' do
              expect(result.matched_count).to eq(2)
            end

            context 'when there is a mix of updates and matched without an update' do

              let(:requests) do
                [{ update_one: { filter: { a: 0 }, update: { "$set" => { a: 1 }}}},
                 { update_one: { filter: { a: 2 }, update: { "$set" => { a: 2 }}}}]
              end

              let(:result) do
                bulk_write.execute
              end

              before do
                authorized_collection.insert_many([{ a: 0 }, { a: 2 }])
              end

              it 'updates the document' do
                result
                expect(authorized_collection.find(a: { '$lt' => 3 }).count).to eq(2)
              end

              it 'reports the upserted id' do
                expect(result.upserted_ids).to eq([])
              end

              it 'reports the upserted count' do
                expect(result.upserted_count).to eq(0)
              end

              it 'reports the modified count' do
                expect(result.modified_count).to eq(1)
              end

              it 'reports the matched count' do
                expect(result.matched_count).to eq(2)
              end
            end

            context 'when there is a write concern error' do

              it_behaves_like 'bulk write with write concern yielding operation failure'
            end
          end

          context 'when upsert is true' do

            let(:requests) do
              [{ update_one: { filter: { _id: 0 }, update: { "$set" => { name: 'test' }}, upsert: true }},
               { update_one: { filter: { _id: 1 }, update: { "$set" => { name: 'test1' }}, upsert: true }}]
            end

            let(:result) do
              bulk_write.execute
            end

            it 'updates the document' do
              expect(result.modified_count).to eq(0)
              expect(authorized_collection.find(name: { '$in' => ['test', 'test1'] }).count).to eq(2)
            end

            it 'reports the upserted count' do
              expect(result.upserted_count).to eq(2)
            end

            it 'reports the modified count' do
              expect(result.modified_count).to eq(0)
            end

            it 'reports the matched count' do
              expect(result.matched_count).to eq(0)
            end

            it 'reports the upserted id' do
              expect(result.upserted_ids).to eq([0, 1])
            end

            context 'when there is a mix of updates, upsert, and matched without an update' do

              let(:requests) do
                [{ update_one: { filter: { a: 0 }, update: { "$set" => { a: 1 }}}},
                 { update_one: { filter: { a: 2 }, update: { "$set" => { a: 2 }}}},
                 { update_one: { filter: { _id: 3 }, update: { "$set" => { a: 4 }}, upsert: true }}]
              end

              let(:result) do
                bulk_write.execute
              end

              before do
                authorized_collection.insert_many([{ a: 0 }, { a: 2 }])
              end

              it 'updates the documents' do
                result
                expect(authorized_collection.find(a: { '$lt' => 3 }).count).to eq(2)
                expect(authorized_collection.find(a: 4).count).to eq(1)
              end

              it 'reports the upserted id' do
                expect(result.upserted_ids).to eq([3])
              end

              it 'reports the upserted count' do
                expect(result.upserted_count).to eq(1)
              end

              it 'reports the modified count' do
                expect(result.modified_count).to eq(1)
              end

              it 'reports the matched count' do
                expect(result.matched_count).to eq(2)
              end
            end

            context 'when there is a write concern error' do

              it_behaves_like 'bulk write with write concern yielding operation failure'
            end
          end
        end

        context 'when providing a single update many' do

          context 'when the write has a collation specified' do

            before do
              authorized_collection.insert_one(name: 'bang')
              authorized_collection.insert_one(name: 'bang')
            end

            let(:requests) do
              [{ update_many: { filter: { name: 'BANG' },
                                update: { "$set" => { name: 'pong' }},
                                collation: collation }}]
            end

            context 'when the server selected supports collations' do
              min_server_fcv '3.4'

              let!(:result) do
                bulk_write.execute
              end

              it 'applies the collation' do
                expect(authorized_collection.find(name: 'pong').count).to eq(2)
              end

              it 'reports the upserted id' do
                expect(result.upserted_ids).to eq([])
              end

              it 'reports the upserted count' do
                expect(result.upserted_count).to eq(0)
              end

              it 'reports the modified count' do
                expect(result.modified_count).to eq(2)
              end

              it 'reports the matched count' do
                expect(result.matched_count).to eq(2)
              end
            end

            context 'when the server selected does not support collations' do
              max_server_version '3.2'

              it 'raises an exception' do
                expect {
                  bulk_write.execute
                }.to raise_exception(Mongo::Error::UnsupportedCollation)
              end

              context 'when a String key is used' do

                let(:requests) do
                  [{ update_many: { filter: { name: 'BANG' },
                                    update: { "$set" => { name: 'pong' }},
                                    'collation' => collation }}]
                end

                it 'raises an exception' do
                  expect {
                    bulk_write.execute
                  }.to raise_exception(Mongo::Error::UnsupportedCollation)
                end
              end
            end
          end

          context 'when the write does not have a collation specified' do

            before do
              authorized_collection.insert_one(name: 'bang')
              authorized_collection.insert_one(name: 'bang')
            end

            let(:requests) do
              [{ update_many: { filter: { name: 'BANG' }, update: { "$set" => { name: 'pong' }}}}]
            end

            let!(:result) do
              bulk_write.execute
            end

            it 'does not apply the collation' do
              expect(authorized_collection.find(name: 'pong').count).to eq(0)
            end

            it 'reports the upserted id' do
              expect(result.upserted_ids).to eq([])
            end

            it 'reports the upserted count' do
              expect(result.upserted_count).to eq(0)
            end

            it 'reports the modified count' do
              expect(result.modified_count).to eq(0)
            end

            it 'reports the matched count' do
              expect(result.matched_count).to be(0)
            end
          end

          context 'when upsert is false' do

            let(:requests) do
              [{ update_many: { filter: { a: 0 }, update: { "$set" => { name: 'test' }}}}]
            end

            let(:result) do
              bulk_write.execute
            end

            before do
              authorized_collection.insert_many([{ a: 0 }, { a: 0 }])
            end

            it 'updates the documents' do
              expect(authorized_collection.find(a: 0).count).to eq(2)
            end

            it 'reports the upserted ids' do
              expect(result.upserted_ids).to eq([])
            end

            it 'reports the upserted count' do
              expect(result.upserted_count).to eq(0)
            end

            it 'reports the modified count' do
              expect(result.modified_count).to eq(2)
            end

            it 'reports the matched count' do
              expect(result.matched_count).to eq(2)
            end

            context 'when there is a write concern error' do

              it_behaves_like 'bulk write with write concern yielding operation failure'
            end
          end

          context 'when upsert is true' do

            let(:requests) do
              [{ update_many: { filter: { _id: 0 }, update: { "$set" => { name: 'test' }}, upsert: true }}]
            end

            let(:result) do
              bulk_write.execute
            end

            it 'updates the document' do
              result
              expect(authorized_collection.find(name: 'test').count).to eq(1)
            end

            it 'reports the upserted count' do
              expect(result.upserted_count).to eq(1)
            end

            it 'reports the matched count' do
              expect(result.matched_count).to eq(0)
            end

            it 'reports the modified count' do
              expect(result.modified_count).to eq(0)
            end

            it 'reports the upserted id' do
              expect(result.upserted_ids).to eq([0])
            end

            context 'when there is a write concern error' do

              it_behaves_like 'bulk write with write concern yielding operation failure'
            end
          end
        end
      end

      context 'when the operations need to be split' do

        let(:batch_size) do
          11
        end

        before do
          allow(client.cluster.next_primary).to receive(:max_write_batch_size).and_return(batch_size - 1)
        end

        context 'when a write error occurs' do

          let(:requests) do
            batch_size.times.map do |i|
              { insert_one: { _id: i }}
            end
          end

          let(:error) do
            begin
              bulk_write.execute
            rescue => e
              e
            end
          end

          it 'raises an exception' do
            expect {
              requests.push({ insert_one: { _id: 5 }})
              bulk_write.execute
            }.to raise_error(Mongo::Error::BulkWriteError)
          end

          it 'sets the document index on the error' do
            requests.push({ insert_one: { _id: 5 }})
            expect(error.result['writeErrors'].first['index']).to eq(batch_size)
          end
        end

        context 'when no write errors occur' do

          let(:requests) do
            batch_size.times.map do |i|
              { insert_one: { _id: i }}
            end
          end

          let(:result) do
            bulk_write.execute
          end

          it 'inserts the documents' do
            expect(result.inserted_count).to eq(batch_size)
          end

          it 'combines the inserted ids' do
            expect(result.inserted_ids.size).to eq(batch_size)
          end

          context 'when a session is provided' do

            let(:operation) do
              result
            end

            let(:client) do
              authorized_client
            end

            let(:extra_options) do
              { session: session }
            end

            it_behaves_like 'an operation using a session'
          end

          context 'when retryable writes are supported' do
            min_server_fcv '3.6'
            require_topology :replica_set, :sharded

            # In a multi-shard cluster, retries may go to a different server
            # than original command which these tests are not prepared to handle
            require_no_multi_shard


            let(:subscriber) { EventSubscriber.new }

            let(:client) do
              authorized_client_with_retry_writes.tap do |client|
                # We do not unsubscribe any of these subscribers.
                # This is harmless since they simply store the events in themselves.
                client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
              end
            end

            let(:collection) do
              client[authorized_collection.name]
            end

            let!(:result) do
              bulk_write.execute
            end

            let(:started_events) do
              subscriber.started_events.select do |event|
                event.command['insert']
              end
            end

            let(:first_txn_number) do
              started_events[-2].command['txnNumber'].instance_variable_get(:@integer)
            end

            let(:second_txn_number) do
              started_events[-1].command['txnNumber'].instance_variable_get(:@integer)
            end

            it 'inserts the documents' do
              expect(result.inserted_count).to eq(batch_size)
            end

            it 'combines the inserted ids' do
              expect(result.inserted_ids.size).to eq(batch_size)
            end

            it 'publishes the expected number of events' do
              expect(started_events.length).to eq 2
            end

            it 'increments the transaction number' do
              expect(second_txn_number). to eq(first_txn_number + 1)
            end
          end
        end
      end

      context 'when an operation exceeds the max bson size' do

        let(:requests) do
          5.times.map do |i|
            { insert_one: { _id: i, x: 'y' * 4000000 }}
          end
        end

        let(:result) do
          bulk_write.execute
        end

        it 'inserts the documents' do
          expect(result.inserted_count).to eq(5)
        end

        context 'when a session is provided' do

          let(:operation) do
            result
          end

          let(:client) do
            authorized_client
          end

          let(:extra_options) do
            { session: session }
          end

          it_behaves_like 'an operation using a session'
        end
      end
    end

    context 'when the bulk write is unordered' do

      let(:bulk_write) do
        described_class.new(collection, requests, options)
      end

      let(:options) do
        { ordered: false }.merge(extra_options)
      end

      let(:extra_options) do
        {}
      end

      let(:bulk_write_invalid_write_concern) do
        described_class.new(collection_invalid_write_concern, requests, options)
      end

      it_behaves_like 'an executable bulk write'
    end

    context 'when the bulk write is ordered' do

      let(:bulk_write) do
        described_class.new(collection, requests, options)
      end

      let(:options) do
        { ordered: true }.merge(extra_options)
      end

      let(:extra_options) do
        {}
      end

      let(:bulk_write_invalid_write_concern) do
        described_class.new(collection_invalid_write_concern, requests, options)
      end

      it_behaves_like 'an executable bulk write'
    end
  end

  describe '#initialize' do

    let(:requests) do
      [{ insert_one: { _id: 0 }}]
    end

    shared_examples_for 'a bulk write initializer' do

      it 'sets the collection' do
        expect(bulk_write.collection).to eq(authorized_collection)
      end

      it 'sets the requests' do
        expect(bulk_write.requests).to eq(requests)
      end
    end

    context 'when no options are provided' do

      let(:bulk_write) do
        described_class.new(authorized_collection, requests)
      end

      it 'sets empty options' do
        expect(bulk_write.options).to be_empty
      end

      it_behaves_like 'a bulk write initializer'
    end

    context 'when options are provided' do

      let(:bulk_write) do
        described_class.new(authorized_collection, requests, ordered: true)
      end

      it 'sets the options' do
        expect(bulk_write.options).to eq(ordered: true)
      end
    end

    context 'when nil options are provided' do

      let(:bulk_write) do
        described_class.new(authorized_collection, requests, nil)
      end

      it 'sets empty options' do
        expect(bulk_write.options).to be_empty
      end
    end
  end

  describe '#ordered?' do

    context 'when no option provided' do

      let(:bulk_write) do
        described_class.new(authorized_collection, [])
      end

      it 'returns true' do
        expect(bulk_write).to be_ordered
      end
    end

    context 'when the option is provided' do

      context 'when the option is true' do

        let(:options) do
          { ordered: true }
        end

        let(:bulk_write) do
          described_class.new(authorized_collection, [], options)
        end

        it 'returns true' do
          expect(bulk_write).to be_ordered
        end
      end

      context 'when the option is false' do

        let(:options) do
          { ordered: false }
        end

        let(:bulk_write) do
          described_class.new(authorized_collection, [], options)
        end

        it 'returns false' do
          expect(bulk_write).to_not be_ordered
        end
      end
    end
  end

  describe 'when the collection has a validator' do
    min_server_fcv '3.2'

    let(:collection_with_validator) do
      authorized_client[:validating,
                        :validator => { :a => { '$exists' => true } }].tap do |c|
        c.create
      end
    end

    before do
      begin; authorized_client[:validating].drop; rescue; end
      collection_with_validator.delete_many
      collection_with_validator.insert_many([{ :a => 1 }, { :a => 2 }])
    end

    context 'when the documents are invalid' do

      let(:ops) do
        [
            { insert_one: { :x => 1 } },
            { update_one: { filter: { :a => 1 },
                            update: { '$unset' => { :a => '' } } } },
            { replace_one: { filter: { :a => 2 },
                             replacement: { :x => 2 } } }
        ]
      end

      context 'when bypass_document_validation is not set' do

        let(:result) do
          collection_with_validator.bulk_write(ops)
        end

        it 'raises BulkWriteError' do
          expect {
            result
          }.to raise_exception(Mongo::Error::BulkWriteError)
        end
      end

      context 'when bypass_document_validation is true' do

        let(:result2) do
          collection_with_validator.bulk_write(
              ops, :bypass_document_validation => true)
        end

        it 'executes successfully' do
          expect(result2.modified_count).to eq(2)
          expect(result2.inserted_count).to eq(1)
        end
      end
    end
  end
end
