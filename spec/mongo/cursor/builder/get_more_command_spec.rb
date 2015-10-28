require 'spec_helper'

describe Mongo::Cursor::Builder::GetMoreCommand do

  describe '#specification' do

    let(:reply) do
      Mongo::Protocol::Reply.allocate
    end

    let(:result) do
      Mongo::Operation::Result.new(reply)
    end

    let(:cursor) do
      Mongo::Cursor.new(view, result, authorized_primary)
    end

    let(:builder) do
      described_class.new(cursor)
    end

    let(:specification) do
      builder.specification
    end

    let(:selector) do
      specification[:selector]
    end

    shared_examples_for 'a getmore command builder' do

      it 'includes the database name' do
        expect(specification[:db_name]).to eq(TEST_DB)
      end

      it 'includes getmore with cursor id' do
        expect(selector[:getMore]).to eq(cursor.id)
      end

      it 'includes the collection name' do
        expect(selector[:collection]).to eq(TEST_COLL)
      end
    end

    context 'when the query is standard' do

      let(:view) do
        Mongo::Collection::View.new(authorized_collection)
      end

      it_behaves_like 'a getmore command builder'

      it 'does not include max time' do
        expect(selector[:maxTimeMS]).to be_nil
      end

      it 'does not include batch size' do
        expect(selector[:batchSize]).to be_nil
      end
    end

    context 'when the query has a batch size' do

      let(:view) do
        Mongo::Collection::View.new(authorized_collection, {}, batch_size: 10)
      end

      it_behaves_like 'a getmore command builder'

      it 'does not include max time' do
        expect(selector[:maxTimeMS]).to be_nil
      end

      it 'includes batch size' do
        expect(selector[:batchSize]).to eq(10)
      end
    end

    context 'when a max await time is specified' do

      context 'when the cursor is not tailable' do

        let(:view) do
          Mongo::Collection::View.new(authorized_collection, {}, max_await_time_ms: 100)
        end

        it_behaves_like 'a getmore command builder'

        it 'does not include max time' do
          expect(selector[:maxTimeMS]).to be_nil
        end

        it 'does not include max await time' do
          expect(selector[:maxAwaitTimeMS]).to be_nil
        end

        it 'does not include batch size' do
          expect(selector[:batchSize]).to be_nil
        end
      end

      context 'when the cursor is tailable' do

        context 'when await data is true' do

          let(:view) do
            Mongo::Collection::View.new(
              authorized_collection,
              {},
              await_data: true,
              tailable: true,
              max_await_time_ms: 100
            )
          end

          it_behaves_like 'a getmore command builder'

          it 'includes max time' do
            expect(selector[:maxTimeMS]).to eq(100)
          end

          it 'does not include max await time' do
            expect(selector[:maxAwaitTimeMS]).to be_nil
          end

          it 'does not include batch size' do
            expect(selector[:batchSize]).to be_nil
          end
        end

        context 'when await data is false' do

          let(:view) do
            Mongo::Collection::View.new(
              authorized_collection,
              {},
              tailable: true,
              max_await_time_ms: 100
            )
          end

          it_behaves_like 'a getmore command builder'

          it 'does not include max time' do
            expect(selector[:maxTimeMS]).to be_nil
          end

          it 'does not include max await time' do
            expect(selector[:maxAwaitTimeMS]).to be_nil
          end

          it 'does not include batch size' do
            expect(selector[:batchSize]).to be_nil
          end
        end
      end
    end
  end
end
