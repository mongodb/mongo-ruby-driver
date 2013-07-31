require 'spec_helper'

describe Mongo::Cursor do

  include_context 'shared client'

  let(:more) { 1 }
  let(:no_more) { 0 }
  let(:diff) { 2 }
  let(:b) { proc { |d| d } }

  let(:scope) { Mongo::Scope.new(collection, {}, {}) }
  let(:cursor) do
    stub!
    described_class.new(scope)
  end

  describe '#inspect' do

    it 'returns a string' do
      expect(cursor.inspect).to be_a(String)
    end

    it 'returns a string containing the scope inspect string' do
      expect(cursor.inspect).to match(/.*#{scope.inspect}.*/)
    end

  end

  describe 'special fields' do

    context 'when the query has special fields' do
      let(:scope) { Mongo::Scope.new(collection, {}, { :comment => 'test' }) }
      let(:results) do
        { :cursor_id => no_more,
          :nreturned => 5,
          :docs => (0...5).to_a }
      end
      before(:each) do
        allow(connection).to receive(:send_and_receive) { [results, node] }
      end

      it 'creates a special selector with $query' do
        expect(Mongo::Protocol::Query).to receive(:new) do |a, b, selector, c|
          expect(selector[:$query]).to eq(scope.selector)
        end
        cursor.each(&b)
      end

    end

  end

  describe '#each' do

    context 'when a block is provided' do
      let(:n_docs) { 5 }
      let(:results) do
        { :cursor_id => no_more,
          :nreturned => n_docs,
          :docs => (0...n_docs).to_a }
      end
      before(:each) do
        allow(connection).to receive(:send_and_receive) { [results, node] }
      end

      it 'yields each doc to the block' do
        expect do |b|
          cursor.each(&b)
        end.to yield_control.exactly(n_docs).times
      end

    end

  end

  describe 'iteration' do
    before(:each) do
      allow(connection).to receive(:send_and_receive) { [results, node] }
    end

    context 'when the query has a limit' do
      let(:limit) { 5 }
      let(:scope) { Mongo::Scope.new(collection, {}, { :limit => limit }) }

      context 'when all docs are retreived in one request' do
        let(:results) do
          { :cursor_id => no_more,
            :nreturned => limit,
            :docs => (0...limit).to_a }
        end

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
          expect(Mongo::Protocol::KillCursors).not_to receive(:new)
          cursor.each(&b)
        end

      end

      context 'when multiple requests are needed' do
        let(:result1) do
          { :cursor_id => more,
            :nreturned => limit - diff,
            :docs => (0...limit - diff).to_a }
        end
        let(:result2) do
          { :cursor_id => more,
            :nreturned => diff,
            :docs => (0...diff).to_a }
        end
        before(:each) do
          allow(connection).to receive(:send_and_receive).and_return(
            [result1, node], [result2, node])
        end

        it 'requests that number of docs in first query message' do
          expect(Mongo::Protocol::Query).to receive(:new) do |a, b, c, opts|
            expect(opts[:limit]).to eq(limit)
          end
          cursor.each(&b)
        end

        it 'requests the remaining docs in a get more message' do
          expect(Mongo::Protocol::GetMore).to receive(:new) do |a, b, num, c|
            expect(num).to eq(diff)
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
      let(:total_docs) { 6 }

      context 'when all docs are retreived in one request' do
        let(:results) do
          { :cursor_id => no_more,
            :nreturned => total_docs,
            :docs => (0...total_docs).to_a }
        end

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
        let(:result1) do
          { :cursor_id => more,
            :nreturned => total_docs - diff,
            :docs => (0...total_docs - diff).to_a }
        end
        let(:result2) do
          { :cursor_id => no_more,
            :nreturned => diff,
            :docs => (0...diff).to_a }
        end
        before(:each) do
          allow(connection).to receive(:send_and_receive).and_return(
            [result1, node], [result2, node])
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
      let(:scope) { Mongo::Scope.new(collection, {}, { :limit => limit }) }

      context 'when all results are retreived in one request' do
        let(:results) do
          { :cursor_id => no_more,
            :nreturned => limit.abs,
            :docs => (0...limit.abs).to_a }
        end

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
        let(:results) do
          { :cursor_id => no_more,
            :nreturned => limit.abs - diff,
            :docs => (0...limit.abs - diff).to_a }
        end

        it 'does not send a get more message' do
          expect(Mongo::Protocol::GetMore).not_to receive(:new)
          cursor.each(&b)
        end

      end

    end

    context 'when the query has a batch size greater than limit' do
      let(:batch_size) { 6 }
      let(:limit) { 5 }
      let(:scope) do
        Mongo::Scope.new(collection, {}, { :limit => limit,
                                           :batch_size => batch_size })
      end

      context 'when all docs are retreived in one request' do
        let(:results) do
          { :cursor_id => no_more,
            :nreturned => limit,
            :docs => (0...limit).to_a }
        end

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
        let(:result1) do
          { :cursor_id => more,
            :nreturned => limit - diff,
            :docs => (0...limit - diff).to_a }
        end
        let(:result2) do
          { :cursor_id => more,
            :nreturned => diff,
            :docs => (0...diff).to_a }
        end
        before(:each) do
          allow(connection).to receive(:send_and_receive).and_return(
            [result1, node], [result2, node])
        end

        it 'requests the limit in the first query message' do
          expect(Mongo::Protocol::Query).to receive(:new) do |a, b, c, opts|
              expect(opts[:limit]).to eq(limit)
          end
          cursor.each(&b)
        end

        it 'requests the remaining docs in a get more message' do
          expect(Mongo::Protocol::GetMore).to receive(:new) do |a, b, num, c|
            expect(num).to eq(diff)
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
      let(:scope) do
        Mongo::Scope.new(collection, {}, { :limit => limit,
                                           :batch_size => batch_size })
      end
      let(:result1) do
        { :cursor_id => more,
          :nreturned => batch_size,
          :docs => (0...batch_size).to_a }
      end
      let(:result2) do
        { :cursor_id => more,
          :nreturned => batch_size,
          :docs => (0...batch_size).to_a }
      end
      let(:result3) do
        { :cursor_id => more,
          :nreturned => batch_size,
          :docs => (0...batch_size).to_a }
      end
        before(:each) do
          allow(connection).to receive(:send_and_receive).and_return(
            [result1, node],
            [result2, node],
            [result3, node])
        end

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
      let(:scope) do
        Mongo::Scope.new(collection, {}, { :batch_size => batch_size })
      end

      context 'when all docs are retreived in one request' do
        let(:results) do
          { :cursor_id => no_more,
            :nreturned => batch_size,
            :docs => (0...batch_size).to_a }
        end

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
        let(:result1) do
          { :cursor_id => more,
            :nreturned => batch_size,
            :docs => (0...batch_size).to_a }
        end
        let(:result2) do
          { :cursor_id => no_more,
            :nreturned => diff,
            :docs => (0...diff).to_a }
        end
        before(:each) do
          allow(connection).to receive(:send_and_receive).and_return(
            [result1, node], [result2, node])
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
          end.to yield_control.exactly(batch_size + diff).times
        end

        it 'sends a kill cursors message' do
          expect(Mongo::Protocol::KillCursors).not_to receive(:new)
          cursor.each(&b)
        end

      end
    end

  end

end
