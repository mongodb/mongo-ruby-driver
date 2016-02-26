require 'spec_helper'

describe Mongo::Cursor do

  describe '#each' do

    let(:server) do
      view.read.select_server(authorized_client.cluster)
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

      context 'when the initial query retieves all documents' do

        let(:documents) do
          (1..10).map{ |i| { field: "test#{i}" }}
        end

        before do
          authorized_collection.insert_many(documents)
        end

        after do
          authorized_collection.delete_many
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

        after do
          authorized_collection.delete_many
        end

        context 'when a getmore gets a socket error' do

          let(:op) do
            double('operation')
          end

          before do
            expect(cursor).to receive(:get_more_operation).and_return(op).ordered
            expect(op).to receive(:execute).and_raise(Mongo::Error::SocketError).ordered
            expect(cursor).to receive(:get_more_operation).and_call_original.ordered
          end

          it 'iterates the documents' do
            cursor.each do |doc|
              expect(doc).to have_key('field')
            end
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
        authorized_collection.insert_many(documents)
      end

      after do
        authorized_collection.delete_many
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
  end

  describe '#inspect' do

    let(:view) do
      Mongo::Collection::View.new(authorized_collection)
    end

    let(:query_spec) do
      { :selector => {}, :options => {}, :db_name => TEST_DB, :coll_name => TEST_COLL }
    end

    let(:reply) do
      Mongo::Operation::Read::Query.new(query_spec)
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
end
