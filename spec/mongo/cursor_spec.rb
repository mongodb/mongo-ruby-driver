require 'spec_helper'

describe Mongo::Cursor do

  include_context 'shared client'
  include_context 'shared cursor'

  let(:cursor) do
    described_class.new(scope).tap do
      allow(connection).to receive(:send_and_receive).and_return(*responses)
    end
  end

  describe '#inspect' do

    it 'returns a string' do
      expect(cursor.inspect).to be_a(String)
    end

    it 'returns a string containing the scope inspect string' do
      expect(cursor.inspect).to match(/.*#{scope.inspect}.*/)
    end
  end

  context 'when the query has special fields' do
    let(:scope_opts) { { :comment => 'test' } }

    it 'creates a special selector with $query' do
      expect(Mongo::Protocol::Query).to receive(:new) do |a, b, selector, c|
        expect(selector[:$query]).to eq(scope.selector)
      end
      cursor.each(&b)
    end
  end

  context 'mongos' do

    it 'creates a special selector with $query' do
      allow(client).to receive(:mongos?).and_return(true)
      expect(Mongo::Protocol::Query).to receive(:new) do |a, b, selector, c|
        expect(selector[:$query]).to eq(scope.selector)
      end
      cursor.each(&b)
    end
  end

  describe '#each' do

    context 'when a block is provided' do
      let(:n_docs) { 10 }
      let(:responses) { results(0, n_docs) }

      it 'yields each doc to the block' do
        expect do |b|
          cursor.each(&b)
        end.to yield_control.exactly(n_docs).times
      end
    end
  end

  describe 'iteration' do

    context 'when the query has a limit' do
      let(:limit) { 8 }
      let(:scope_opts) { { :limit => limit } }

      context 'when all docs are retreived in one request' do
        let(:responses) { results(0, limit) }

        it 'requests that number of docs in first query message' do
          expect(Mongo::Protocol::Query).to receive(:new) do |a, b, c, opts|
            expect(opts[:limit]).to eq(limit)
          end
          cursor.each(&b)
        end

        it 'returns exactly that number of documents' do
          expect do |b|
            cursor.each(&b)
          end.to yield_control.exactly(limit).times
        end

        it 'does not send a kill cursors message' do
          allow(connection).to receive(:send_and_receive).and_return(results)
          expect(Mongo::Protocol::KillCursors).not_to receive(:new)
          cursor.each(&b)
        end
      end

      context 'when multiple requests are needed' do
        let(:delta) { 2 }
        let(:responses) do
          [results(nonzero, limit - delta),
           results(nonzero, delta)]
        end

        it 'requests that number of docs in first query message' do
          expect(Mongo::Protocol::Query).to receive(:new) do |a, b, c, opts|
            expect(opts[:limit]).to eq(limit)
          end
          cursor.each(&b)
        end

        it 'requests the remaining docs in a get more message' do
          expect(Mongo::Protocol::GetMore).to receive(:new) do |a, b, num, c|
            expect(num).to eq(delta)
          end
          cursor.each(&b)
        end

        it 'returns exactly that number of documents' do
          expect do |b|
            cursor.each(&b)
          end.to yield_control.exactly(limit).times
        end

        it 'sends a kill cursors message' do
          expect(Mongo::Protocol::KillCursors).to receive(:new)
          cursor.each(&b)
        end
      end
    end

    context 'when the query has no limit' do
      let(:total_docs) { 20 }
      let(:delta) { 5 }

      context 'when all docs are retreived in one request' do
        let(:responses) { results(0, total_docs)  }

        it 'does not limit the first query message' do
          expect(Mongo::Protocol::Query).to receive(:new) do |a, b, c, opts|
            expect(opts[:limit]).to eq(nil)
          end
          cursor.each(&b)
        end

        it 'returns all documents matching query' do
          expect do |b|
            cursor.each(&b)
          end.to yield_control.exactly(total_docs).times
        end

        it 'does not send a kill cursors message' do
          expect(Mongo::Protocol::KillCursors).not_to receive(:new)
          cursor.each(&b)
        end
      end

      context 'when multiple requests are needed' do
        let(:responses) do
          [results(nonzero, total_docs - delta), results(0, delta)]
        end

        it 'does not limit the first query message' do
          expect(Mongo::Protocol::Query).to receive(:new) do |a, b, c, opts|
            expect(opts[:limit]).to eq(nil)
          end
          cursor.each(&b)
        end

        it 'does not limit the get more message' do
          expect(Mongo::Protocol::GetMore).to receive(:new) do |a, b, num, c|
            expect(num).to eq(nil)
          end
          cursor.each(&b)
        end

        it 'returns the number of documents matching the query' do
          expect do |b|
            cursor.each(&b)
          end.to yield_control.exactly(total_docs).times
        end

        it 'does not send a kill cursors message' do
          expect(Mongo::Protocol::KillCursors).not_to receive(:new)
          cursor.each(&b)
        end
      end
    end

    context 'when the query has a negative limit' do
      let(:limit) { -5 }
      let(:scope_opts) { { :limit => limit } }

      context 'when all results are retreived in one request' do
        let(:responses) { results(0, limit.abs)  }

        it 'requests that number of docs in the first query message' do
          expect(Mongo::Protocol::Query).to receive(:new) do |a, b, c, opts|
            expect(opts[:limit]).to eq(limit)
          end
          cursor.each(&b)
        end

        it 'returns exactly that limit number of documents' do
          expect do |b|
            cursor.each(&b)
          end.to yield_control.exactly(limit.abs).times
        end

        it 'does not send a get more message' do
          expect(Mongo::Protocol::GetMore).not_to receive(:new)
          cursor.each(&b)
        end

        it 'does not send a kill cursors message' do
          expect(Mongo::Protocol::KillCursors).not_to receive(:new)
          cursor.each(&b)
        end
      end

      context 'when not all results are returned in one request' do
        let(:delta) { 2 }
        let(:responses) { results(0, limit.abs - delta)  }

        it 'does not send a get more message' do
          expect(Mongo::Protocol::GetMore).not_to receive(:new)
          cursor.each(&b)
        end
      end
    end

    context 'when the query has a batch size greater than limit' do
      let(:batch_size) { 6 }
      let(:limit) { 5 }
      let(:scope_opts) { { :limit => limit, :batch_size => batch_size } }

      context 'when all docs are retreived in one request' do
        let(:responses) { results(0, limit) }

        it 'requests the limit number of docs in first query message' do
          expect(Mongo::Protocol::Query).to receive(:new) do |a, b, c, opts|
            expect(opts[:limit]).to eq(limit)
          end
          cursor.each(&b)
        end

        it 'does not send a get more message' do
          expect(Mongo::Protocol::GetMore).not_to receive(:new)
          cursor.each(&b)
        end

        it 'returns exactly that limit number of documents' do
          expect do |b|
            cursor.each(&b)
          end.to yield_control.exactly(limit).times
        end

        it 'does not send a kill cursors message' do
          expect(Mongo::Protocol::KillCursors).not_to receive(:new)
          cursor.each(&b)
        end
      end

      context 'when multiple requests are needed' do
        let(:delta) { 2 }
        let(:responses) do
          [results(nonzero, limit - delta),
           results(nonzero, delta)]
        end

        it 'requests the limit in the first query message' do
          expect(Mongo::Protocol::Query).to receive(:new) do |a, b, c, opts|
            expect(opts[:limit]).to eq(limit)
          end
          cursor.each(&b)
        end

        it 'requests the remaining docs in a get more message' do
          expect(Mongo::Protocol::GetMore).to receive(:new) do |a, b, num, c|
            expect(num).to eq(delta)
          end
          cursor.each(&b)
        end

        it 'returns exactly that limit number of documents' do
          expect do |b|
            cursor.each(&b)
          end.to yield_control.exactly(limit).times
        end

        it 'sends a kill cursors message' do
          expect(Mongo::Protocol::KillCursors).to receive(:new)
          cursor.each(&b)
        end
      end
    end

    context 'when the query has a limit greater than batch size' do
      let(:limit) { 15 }
      let(:batch_size) { 5 }
      let(:scope_opts) { { :limit => limit, :batch_size => batch_size } }
      let(:responses) { [results(nonzero, batch_size) * 3] }

      it 'requests the batch size in the first query message' do
        expect(Mongo::Protocol::Query).to receive(:new) do |a, b, c, opts|
          expect(opts[:limit]).to eq(batch_size)
        end
        cursor.each(&b)
      end

      it 'requests the batch size in each get more message' do
        expect(Mongo::Protocol::GetMore).to receive(:new) do |a, b, num, c|
          expect(num).to eq(batch_size)
        end
        expect(Mongo::Protocol::GetMore).to receive(:new) do |a, b, num, c|
          expect(num).to eq(batch_size)
        end
        cursor.each(&b)
      end

      it 'returns exactly that limit number of documents' do
        expect do |b|
          cursor.each(&b)
        end.to yield_control.exactly(limit).times
      end

      it 'sends a kill cursors message' do
        expect(Mongo::Protocol::KillCursors).to receive(:new)
        cursor.each(&b)
      end
    end

    context 'when the query has a batch size set but no limit' do
      let(:batch_size) { 6 }
      let(:scope_opts) { { :batch_size => batch_size } }

      context 'when all docs are retreived in one request' do
        let(:responses) { results(0, batch_size) }

        it 'requests the batch size in the first query message' do
          expect(Mongo::Protocol::Query).to receive(:new) do |a, b, c, opts|
            expect(opts[:limit]).to eq(batch_size)
          end
          cursor.each(&b)
        end

        it 'does not send a get more message' do
          expect(Mongo::Protocol::GetMore).not_to receive(:new)
          cursor.each(&b)
        end

        it 'returns exactly that batch size number of documents' do
          expect do |b|
            cursor.each(&b)
          end.to yield_control.exactly(batch_size).times
        end

        it 'does not send a kill cursors message' do
          expect(Mongo::Protocol::KillCursors).not_to receive(:new)
          cursor.each(&b)
        end
      end

      context 'when multiple requests are needed' do
        let(:remaining) { 2 }
        let(:responses) do
          [results(nonzero, batch_size),
           results(0, remaining)]
        end

        it 'requests the batch size in the first query message' do
          expect(Mongo::Protocol::Query).to receive(:new) do |a, b, c, opts|
            expect(opts[:limit]).to eq(batch_size)
          end
          cursor.each(&b)
        end

        it 'requests the batch size in a get more message' do
          expect(Mongo::Protocol::GetMore).to receive(:new) do |a, b, num, c|
            expect(num).to eq(batch_size)
          end
          cursor.each(&b)
        end

        it 'returns the number of documents matching the query' do
          expect do |b|
            cursor.each(&b)
          end.to yield_control.exactly(batch_size + remaining).times
        end

        it 'sends a kill cursors message' do
          expect(Mongo::Protocol::KillCursors).not_to receive(:new)
          cursor.each(&b)
        end
      end
    end
  end
end
