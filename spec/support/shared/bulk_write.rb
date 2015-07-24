shared_examples 'a bulk write object' do

  context 'when no operations are provided' do

    let(:operations) do
      []
    end

    it 'raises an error' do
      expect {
        bulk.execute
      }.to raise_error(ArgumentError)
    end
  end

  context 'when invalid operations are provided' do

    let(:operations) do
      [{ :not_an_op => {}}]
    end

    it 'raises an error' do
      expect {
        bulk.execute
      }.to raise_error(Mongo::Error::InvalidBulkOperationType)
    end
  end

  context 'when an insert_one operation is provided' do

    context 'when there is a write failure' do

      let(:operations) do
        [{ insert_one: { _id: 1 }}, { insert_one: { _id: 1 }}]
      end

      it 'raises a BulkWriteError' do
        expect {
          bulk.execute
        }.to raise_error(Mongo::Error::BulkWriteError)
      end
    end

    context 'when a document is provided' do

      let(:operations) do
        [{ insert_one: { name: 'test' }}]
      end

      it 'returns n_inserted of 1' do
        expect(bulk.execute.inserted_count).to eq(1)
      end

      it 'only inserts that document' do
        bulk.execute
        expect(authorized_collection.find.first['name']).to eq('test')
      end

      context 'when there is a write concern error' do

        context 'when the server version is < 2.6' do

          it 'raises a BulkWriteError', if: !write_command_enabled?  && standalone? do
            expect {
              bulk_invalid_write_concern.execute
            }.to raise_error(Mongo::Error::BulkWriteError)
          end
        end

        context 'when the server version has write commands enabled' do

          it 'raises an OperationFailure', if: write_command_enabled? && standalone? do
            expect {
              bulk_invalid_write_concern.execute
            }.to raise_error(Mongo::Error::OperationFailure)
          end
        end
      end
    end

    context 'when an invalid object is provided' do

      let(:operations) do
        [{ insert_one: [] }]
      end

      it 'raises an exception' do
        expect {
          bulk.execute
        }.to raise_error(Mongo::Error::InvalidBulkOperation)
      end
    end
  end

  context 'delete_one' do

    let(:docs) do
      [ { a: 1 }, { a: 1 } ]
    end

    let(:expected) do
      [{ 'a' => 1 }]
    end

    before do
      authorized_collection.insert_many(docs)
    end

    let(:operations) do
      [ { delete_one: { a: 1 }},
        { delete_one: { a: 2 }}
      ]
    end

    context 'when no selector is specified' do

      let(:operations) do
        [{ delete_one: nil }]
      end

      it 'raises an exception' do
        expect {
          bulk.execute
        }.to raise_exception(Mongo::Error::InvalidBulkOperation)
      end
    end

    context 'when multiple documents match delete selector' do

      it 'reports n_removed correctly' do
        expect(bulk.execute.deleted_count).to eq(1)
      end

      it 'deletes only matching documents' do
        bulk.execute
        expect(authorized_collection.find.projection(_id: 0).to_a).to eq(expected)
      end

      context 'when there is a write concern error' do

        context 'when the server version is < 2.6' do

          it 'raises a BulkWriteError', if: !write_command_enabled? && standalone? do
            expect {
              bulk_invalid_write_concern.execute
            }.to raise_error(Mongo::Error::BulkWriteError)
          end
        end

        context 'when the server version has write commands enabled' do

          it 'raises an OperationFailure', if: write_command_enabled? && standalone? do
            expect {
              bulk_invalid_write_concern.execute
            }.to raise_error(Mongo::Error::OperationFailure)
          end
        end
      end
    end
  end

  context 'when a delete_many operation is provided' do

    let(:docs) do
      [{ a: 1 }, { a: 1 }]
    end

    before do
      authorized_collection.insert_many(docs)
    end

    let(:operations) do
      [{ delete_many: { a: 1 }}]
    end

    context 'when no selector is specified' do

      let(:operations) do
        [{ delete_many: nil }]
      end

      it 'raises an exception' do
        expect {
          bulk.execute
        }.to raise_exception(Mongo::Error::InvalidBulkOperation)
      end
    end

    context 'when a selector is specified' do

      context 'when multiple documents match delete selector' do

        it 'reports n_removed correctly' do
          expect(bulk.execute.deleted_count).to eq(2)
        end

        it 'deletes all matching documents' do
          bulk.execute
          expect(authorized_collection.find.to_a).to be_empty
        end

        context 'when there is a write concern error' do

          context 'when the server version is < 2.6' do

            it 'raises a BulkWriteError', if: !write_command_enabled? && standalone? do
              expect {
                bulk_invalid_write_concern.execute
              }.to raise_error(Mongo::Error::BulkWriteError)
            end
          end

          context 'when the server version has write commands enabled' do

            it 'raises an OperationFailure', if: write_command_enabled? && standalone? do
              expect {
                bulk_invalid_write_concern.execute
              }.to raise_error(Mongo::Error::OperationFailure)
            end
          end
        end
      end

      context 'when only one document matches delete selector' do

        let(:docs) do
          [{ a: 1 }, { a: 2 }]
        end

        let(:expected) do
          [{ 'a' => 2 }]
        end

        it 'reports n_removed correctly' do
          expect(bulk.execute.deleted_count).to eq(1)
        end

        it 'deletes all matching documents' do
          bulk.execute
          expect(authorized_collection.find.projection(_id: 0).to_a).to eq(expected)
        end
      end
    end
  end

  context 'when a replace_one operation is provided' do

    let(:docs) do
      [{ a: 1 }, { a: 1 }]
    end

    let(:expected) do
      [{ 'a' => 2 }, { 'a' => 1 }]
    end

    before do
      authorized_collection.insert_many(docs)
    end

    let(:replacement) do
      { a: 2 }
    end

    let(:operations) do
      [{ replace_one: { find: { a: 1 },
                        replacement: replacement,
                        upsert: false }
      }]
    end

    context 'when a replace document is not specified' do

      let(:operations) do
        [{ replace_one: { find: { a: 1 },
                          replacement: nil,
                          upsert: false }
        }]
      end

      it 'raises an exception' do
        expect {
          bulk.execute
        }.to raise_exception(Mongo::Error::InvalidBulkOperation)
      end
    end

    context 'when there are $-operator top-level keys' do

      let(:operations) do
        [{ replace_one: { find: { a: 1 },
                          replacement: { :$set => { a: 3 }},
                          upsert: false }
        }]
      end

      it 'raises an exception' do
        expect {
          bulk.execute
        }.to raise_exception(Mongo::Error::InvalidBulkOperation)
      end

    end

    context 'when a replace document is specified' do

      it 'applies the replacement to only one matching document' do
        bulk.execute
        expect(authorized_collection.find(replacement).count).to eq(1)
      end

      it 'reports n_matched correctly' do
        expect(bulk.execute.matched_count).to eq(1)
      end

      it 'only applies the replacement to one matching document' do
        bulk.execute
        expect(authorized_collection.find.projection(_id: 0).to_a).to eq(expected)
      end

      context 'when there is a write concern error' do

        context 'when the server version is < 2.6' do

          it 'raises a BulkWriteError', if: !write_command_enabled? && standalone? do
            expect {
              bulk_invalid_write_concern.execute
            }.to raise_error(Mongo::Error::BulkWriteError)
          end
        end

        context 'when the server version has write commands enabled' do

          it 'raises an OperationFailure', if: write_command_enabled? && standalone? do
            expect {
              bulk_invalid_write_concern.execute
            }.to raise_error(Mongo::Error::OperationFailure)
          end
        end
      end

      context 'when upsert is true' do

        let(:operations) do
          [{ replace_one: { find: { a: 4 },
                            replacement: replacement,
                            upsert: true }
          }]
        end

        let(:expected) do
          [{ 'a' => 1 }, { 'a' => 1 }, { 'a' => 2 }]
        end

        it 'upserts the replacement document' do
          bulk.execute
          expect(authorized_collection.find(replacement).count).to eq(1)
        end

        it 'reports n_matched correctly' do
          expect(bulk.execute.matched_count).to eq(0)
        end

        it 'does not replace any documents' do
          bulk.execute
          expect(authorized_collection.find.projection(_id: 0).to_a).to eq(expected)
        end
      end
    end
  end

  context 'when an update_one operation is provided' do

    let(:docs) do
      [{ a: 1 }, { a: 1 }]
    end

    let(:update) do
      { :$set => { a: 2 }}
    end

    let(:operations) do
      [{ update_one: { find: { a: 1 },
                       update: update,
                       upsert: false }
      }]
    end

    before do
      authorized_collection.insert_many(docs)
    end

    let(:expected) do
      [{ 'a' => 2 },  { 'a' => 1 }]
    end

    context 'when there is a write failure' do

      let(:operations) do
        [{ update_one: { find: { a: 1 },
                         update: { '$st' => { field: 'blah' } },
                         upsert: false }
         }]
      end

      it 'raises a BulkWriteError' do
        expect {
          bulk.execute
        }.to raise_error(Mongo::Error::BulkWriteError)
      end
    end

    context 'when an update document is not specified' do

      let(:operations) do
        [{ update_one: [{ a: 1 }]}]
      end

      let(:operations) do
        [{ update_one: { find: { a: 1 },
                         upsert: false }
        }]
      end

      it 'raises an exception' do
        expect {
          bulk.execute
        }.to raise_exception(Mongo::Error::InvalidBulkOperation)
      end
    end

    context 'when an invalid update document is specified' do

      let(:update) do
        { a: 2 }
      end

      it 'raises an exception' do
        expect {
          bulk.execute
        }.to raise_exception(Mongo::Error::InvalidBulkOperation)
      end
    end

    context 'when a valid update document is specified' do

      it 'reports n_modified correctly', if: write_command_enabled?  do
        expect(bulk.execute.modified_count).to eq(1)
      end

      it 'reports n_modified correctly', unless: write_command_enabled?  do
        expect(bulk.execute.modified_count).to eq(nil)
      end

      it 'reports n_upserted correctly' do
        expect(bulk.execute.upserted_count).to eq(0)
      end

      it 'reports n_matched correctly' do
        expect(bulk.execute.matched_count).to eq(1)
      end

      it 'applies the correct writes' do
        bulk.execute
        expect(authorized_collection.find.projection(_id: 0).to_a).to eq(expected)
      end

      context 'when there is a write concern error' do

        context 'when the server version is < 2.6' do

          it 'raises a BulkWriteError', if: !write_command_enabled? && standalone? do
            expect {
              bulk_invalid_write_concern.execute
            }.to raise_error(Mongo::Error::BulkWriteError)
          end
        end

        context 'when the server version has write commands enabled' do

          it 'raises an OperationFailure', if: write_command_enabled? && standalone? do
            expect {
              bulk_invalid_write_concern.execute
            }.to raise_error(Mongo::Error::OperationFailure)
          end
        end
      end

      context 'when upsert is true' do

        let(:operations) do
          [{ update_one: { find: { a: 3 },
                           update: update,
                           upsert: true }
          }]
        end

        let(:expected) do
          [{ 'a' => 1 },  { 'a' => 1 }, { 'a' => 2 }]
        end

        it 'reports n_modified correctly', if: write_command_enabled?  do
          expect(bulk.execute.modified_count).to eq(0)
        end

        it 'reports n_modified correctly', unless: write_command_enabled?  do
          expect(bulk.execute.modified_count).to eq(nil)
        end

        it 'reports n_upserted correctly' do
          expect(bulk.execute.upserted_count).to eq(1)
        end

        it 'returns the upserted ids', if: write_command_enabled? do
          expect(bulk.execute.upserted_ids.size).to eq(1)
        end

        it 'reports n_matched correctly' do
          expect(bulk.execute.matched_count).to eq(0)
        end

        it 'applies the correct writes' do
          bulk.execute
          expect(authorized_collection.find.projection(_id: 0).to_a).to eq(expected)
        end
      end
    end
  end

  context 'when an update_many operation is provided' do

    let(:docs) do
      [{ a: 1 }, { a: 1 }]
    end

    let(:update) do
      { :$set => { a: 2 }}
    end

    let(:operations) do
      [{ update_many: { find: { a: 1 },
                        update: update,
                        upsert: false }
      }]
    end

    let(:expected) do
      [{ 'a' => 2 },  { 'a' => 2 }]
    end

    before do
      authorized_collection.insert_many(docs)
    end

    context 'when there is a write failure' do

      let(:operations) do
        [{ update_many: { find: { a: 1 },
                          update: { '$st' => { field: 'blah' } },
                          upsert: false }
         }]
      end

      it 'raises an BulkWriteError' do
        expect {
          bulk.execute
        }.to raise_error(Mongo::Error::BulkWriteError)
      end
    end

    context 'when an update document is not specified' do

      let(:operations) do
        [{ update_many: { find: { a: 1 },
                          upsert: false }
        }]
      end

      it 'raises an exception' do
        expect do
          bulk.execute
        end.to raise_exception(Mongo::Error::InvalidBulkOperation)
      end
    end

    context 'when an invalid update document is specified' do

      let(:update) do
        { a: 2 }
      end

      it 'raises an exception' do
        expect do
          bulk.execute
        end.to raise_exception(Mongo::Error::InvalidBulkOperation)
      end
    end

    context 'when a valid update document is specified' do

      it 'reports n_modified correctly', if: write_command_enabled?  do
        expect(bulk.execute.modified_count).to eq(2)
      end

      it 'reports n_modified correctly', unless: write_command_enabled?  do
        expect(bulk.execute.modified_count).to eq(nil)
      end

      it 'reports n_upserted correctly' do
        expect(bulk.execute.upserted_count).to eq(0)
      end

      it 'reports n_matched correctly' do
        expect(bulk.execute.matched_count).to eq(2)
      end

      it 'applies the correct writes' do
        bulk.execute
        expect(authorized_collection.find.projection(_id: 0).to_a).to eq(expected)
      end

      context 'when there is a write concern error' do

        context 'when the server version is < 2.6' do

          it 'raises a BulkWriteError', if: !write_command_enabled? && standalone? do
            expect {
              bulk_invalid_write_concern.execute
            }.to raise_error(Mongo::Error::BulkWriteError)
          end
        end

        context 'when the server version has write commands enabled' do

          it 'raises an OperationFailure', if: write_command_enabled? && standalone? do
            expect {
              bulk_invalid_write_concern.execute
            }.to raise_error(Mongo::Error::OperationFailure)
          end
        end
      end

      context 'when upsert is true' do

        let(:operations) do
          [{ update_many: { find: { a: 3 },
                            update: update,
                            upsert: true }
          }]
        end

        let(:expected) do
          [ { 'a' => 1 },  { 'a' => 1 }, { 'a' => 2 } ]
        end

        it 'reports n_modified correctly', if: write_command_enabled?  do
          expect(bulk.execute.modified_count).to eq(0)
        end

        it 'reports n_modified correctly', unless: write_command_enabled?  do
          expect(bulk.execute.modified_count).to eq(nil)
        end

        it 'reports n_upserted correctly' do
          expect(bulk.execute.upserted_count).to eq(1)
        end

        it 'reports n_matched correctly' do
          expect(bulk.execute.matched_count).to eq(0)
        end

        it 'applies the correct writes' do
          bulk.execute
          expect(authorized_collection.find.projection(_id: 0).to_a).to eq(expected)
        end
      end
    end
  end

  context 'when the operations need to be split' do

    before do
      6000.times do |i|
        authorized_collection.insert_one(x: i)
      end
    end

    let(:operations) do
      [].tap do |ops|
        3000.times do |i|
          ops << { update_one: { find: { x: i },
                                 update: { '$set' => { x: 6000-i } },
                                 upsert: false }
                 }
        end
        ops << { :insert_one => { test: 'emily' } }
        3000.times do |i|
          ops << { update_one: { find:  { x: 3000+i },
                                 update: { '$set' => { x: 3000-i } },
                                 upsert: false }
                 }
        end
      end
    end

    it 'completes all operations' do
      bulk.execute
      expect(authorized_collection.find(x: { '$lte' => 3000 }).to_a.size).to eq(3000)
    end

    context 'when there is a write concern error' do

      context 'when the server version is < 2.6' do

        it 'raises a BulkWriteError', if: !write_command_enabled? && standalone? do
          expect {
            bulk_invalid_write_concern.execute
          }.to raise_error(Mongo::Error::BulkWriteError)
        end
      end

      context 'when the server version has write commands enabled' do

        it 'raises an OperationFailure', if: write_command_enabled? && standalone? do
          expect {
            bulk_invalid_write_concern.execute
          }.to raise_error(Mongo::Error::OperationFailure)
        end
      end
    end
  end
end
