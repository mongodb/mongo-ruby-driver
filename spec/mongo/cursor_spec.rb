require 'spec_helper'

describe Mongo::Cursor do

  include_context 'shared cursor'

  let(:server) { double('server') }

  let(:cursor) { described_class.new(view, response) }

  describe '#inspect' do

    it 'returns a string' do
      expect(cursor.inspect).to be_a(String)
    end

    it 'returns a string containing the collection view inspect string' do
      expect(cursor.inspect).to match(/.*#{view.inspect}.*/)
    end
  end

  describe '#each' do

    context 'when a block is provided' do
      let(:n_docs) { 10 }
      let(:response) { make_response(0, n_docs) }

      it 'yields each doc to the block' do
        #expect do |b|
        #  cursor.each(&b)
        #end.to yield_control.exactly(n_docs).times
      end
    end
  end

  describe 'iteration' do

    context 'when the query has a limit' do
      let(:limit) { 8 }
      let(:view_opts) { { :limit => limit } }

      context 'when all docs are retrieved in one request' do
        let(:response) { make_response(0, limit) }

        it 'yields with exactly that number of documents' do
          #expect do |b|
          #  cursor.each(&b)
          #end.to yield_control.exactly(limit).times
        end

        it 'does not send a kill cursors message' do
          # @todo: uncomment
          #expect(Mongo::Operation::KillCursors).not_to receive(:new)
          #cursor.each(&b)
        end
      end

      context 'when multiple requests are needed' do
        let(:delta) { 2 }
        let(:response) { make_response(nonzero, limit - delta) }
        let(:get_mores) { [ make_response(0, delta) ] }

        it 'requests the remaining docs in a get more message' do
          # @todo: uncomment
          #expect(Mongo::Operation::Read::GetMore).to receive(:new) do |spec, cxt|
          #  expect(spec[:to_return]).to eq(delta)
          #end
          #cursor.each(&b)
        end

        it 'yields with exactly that number of documents' do
          #expect do |b|
          #  cursor.each(&b)
          #end.to yield_control.exactly(limit).times
        end

        it 'sends a kill cursors message' do
          # @todo: uncomment
          #expect(Mongo::Operation::KillCursors).to receive(:new)
          #cursor.each(&b)
        end
      end
    end

    context 'when the query has no limit' do
      let(:total_docs) { 20 }
      let(:delta) { 5 }

      context 'when all docs are retrieved in one request' do
        let(:response) { make_response(0, total_docs)  }

        it 'returns yields all documents matching query' do
          #expect do |b|
          #  cursor.each(&b)
          #end.to yield_control.exactly(total_docs).times
        end

        it 'does not send a kill cursors message' do
          # @todo: uncomment
          #expect(Mongo::Operation::KillCursors).not_to receive(:new)
          #cursor.each(&b)
        end
       end

      context 'when multiple requests are needed' do
        let(:response) { make_response(nonzero, total_docs - delta)  }
        let(:get_mores) { [ make_response(0, delta) ] }

        it 'does not limit the get more message' do
          #expect(Mongo::Operation::Read::GetMore).to receive(:new) do |spec, cxt|
          #  expect(spec[:to_return]).to eq(nil)
          #end
          #cursor.each(&b)
        end

        it 'returns the number of documents matching the query' do
          #expect do |b|
          #  cursor.each(&b)
          #end.to yield_control.exactly(total_docs).times
        end

        it 'does not send a kill cursors message' do
          # @todo: uncomment
          #expect(Mongo::Operation::KillCursors).not_to receive(:new)
          #cursor.each(&b)
        end
      end
    end

    context 'when the query has a negative limit' do
      let(:limit) { -5 }
      let(:view_opts) { { :limit => limit } }

      context 'when all results are retrieved in one request' do
        let(:response) { make_response(0, limit.abs)  }

         it 'yields exactly that limit number of documents' do
           #expect do |b|
           #  cursor.each(&b)
           #end.to yield_control.exactly(limit.abs).times
         end

         it 'does not send a get more message' do
           # @todo: uncomment
           #expect(Mongo::Operation::Read::GetMore).not_to receive(:new)
           #cursor.each(&b)
         end

         it 'does not send a kill cursors message' do
           # @todo: uncomment
           #expect(Mongo::Operation::KillCursors).not_to receive(:new)
           #cursor.each(&b)
         end
      end

      context 'when not all results are returned in one request' do
        let(:delta) { 2 }
        let(:response) { make_response(0, limit.abs - delta)  }

        it 'does not send a get more message' do
          # @todo: uncomment
          #expect(Mongo::Operation::Read::GetMore).not_to receive(:new)
          #cursor.each(&b)
        end
      end
    end

    context 'when the query has a batch size greater than limit' do
      let(:batch_size) { 6 }
      let(:limit) { 5 }
      let(:view_opts) { { :limit => limit, :batch_size => batch_size } }

      context 'when all docs are retrieved in one request' do
        let(:response) { make_response(0, limit)  }

        it 'does not send a get more message' do
          # @todo: uncomment
          #expect(Mongo::Operation::Read::GetMore).not_to receive(:new)
          #cursor.each(&b)
        end

        it 'returns exactly that limit number of documents' do
          #expect do |b|
          #  cursor.each(&b)
          #end.to yield_control.exactly(limit).times
        end

        it 'does not send a kill cursors message' do
          # @todo: uncomment
          #expect(Mongo::Operation::KillCursors).not_to receive(:new)
          #cursor.each(&b)
        end
      end

      context 'when multiple requests are needed' do
        let(:delta) { 2 }
        let(:response) { make_response(nonzero, limit - delta)  }
        let(:get_mores) { [ make_response(nonzero, delta) ] }

        it 'requests the remaining docs in a get more message' do
          # @todo: uncomment
          #expect(Mongo::Operation::Read::GetMore).to receive(:new) do |spec, cxt|
          #  expect(spec[:to_return]).to eq(delta)
          #end
          #cursor.each(&b)
        end

        it 'returns exactly that limit number of documents' do
          #expect do |b|
          #  cursor.each(&b)
          #end.to yield_control.exactly(limit).times
        end

        it 'sends a kill cursors message' do
          # @todo: uncomment
          #expect(Mongo::Operation::KillCursors).to receive(:new)
          #cursor.each(&b)
        end
      end
    end

    context 'when the query has a limit greater than batch size' do
      let(:limit) { 15 }
      let(:batch_size) { 5 }
      let(:view_opts) { { :limit => limit, :batch_size => batch_size } }
      let(:response) { make_response(nonzero, batch_size) }
      let(:get_mores) { [ make_response(nonzero, batch_size),
                          make_response(nonzero, batch_size) ]}

      it 'requests the batch size in each get more message' do
        #expect(Mongo::Operation::Read::GetMore).to receive(:new) do |spec, cxt|
        #  expect(spec[:to_return]).to eq(batch_size)
        #end
        #expect(Mongo::Operation::Read::GetMore).to receive(:new) do |spec, cxt|
        #  expect(spec[:to_return]).to eq(batch_size)
        #end
        #cursor.each(&b)
      end

      it 'returns exactly that limit number of documents' do
        #expect do |b|
        #  cursor.each(&b)
        #end.to yield_control.exactly(limit).times
      end

      it 'sends a kill cursors message' do
        # @todo: uncomment
        #expect(Mongo::Operation::KillCursors).to receive(:new)
        #cursor.each(&b)
      end
    end

    context 'when the query has a batch size set but no limit' do
      let(:batch_size) { 6 }
      let(:view_opts) { { :batch_size => batch_size } }

      context 'when all docs are retrieved in one request' do
        let(:response) { make_response(0, batch_size) }

        it 'does not send a get more message' do
          # @todo: uncomment
          #expect(Mongo::Operation::Read::GetMore).not_to receive(:new)
          #cursor.each(&b)
        end

        it 'returns exactly that batch size number of documents' do
          #expect do |b|
          #  cursor.each(&b)
          #end.to yield_control.exactly(batch_size).times
        end

        it 'does not send a kill cursors message' do
          # @todo: uncomment
          #expect(Mongo::Operation::KillCursors).not_to receive(:new)
          #cursor.each(&b)
        end
      end

      context 'when multiple requests are needed' do
        let(:remaining) { 2 }
        let(:response) { make_response(nonzero, batch_size) }
        let(:get_mores) { [ make_response(0, remaining) ] }

        it 'requests the batch size in a get more message' do
          # @todo: uncomment
          #expect(Mongo::Operation::Read::GetMore).to receive(:new) do |spec, cxt|
          #  expect(spec[:to_return]).to eq(batch_size)
          #end
          #cursor.each(&b)
        end

        it 'returns the number of documents matching the query' do
          #expect do |b|
          #  cursor.each(&b)
          #end.to yield_control.exactly(batch_size + remaining).times
        end

        it 'sends a kill cursors message' do
          # @todo: uncomment
          #expect(Mongo::Operation::KillCursors).not_to receive(:new)
          #cursor.each(&b)
        end
      end
    end
  end
end
