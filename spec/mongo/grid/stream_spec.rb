require 'spec_helper'

describe Mongo::Grid::FSBucket::Stream do

  let(:fs) do
    authorized_client.database.fs
  end

  describe '.get' do

    let(:stream) do
      described_class.get(fs, mode)
    end

    context 'when mode is read' do

      let(:mode) do
        Mongo::Grid::FSBucket::Stream::READ_MODE
      end

      it 'returns a Stream::Read object' do
        expect(stream).to be_a(Mongo::Grid::FSBucket::Stream::Read)
      end
    end

    context 'when mode is write' do

      let(:mode) do
        Mongo::Grid::FSBucket::Stream::WRITE_MODE
      end

      it 'returns a Stream::Write object' do
        expect(stream).to be_a(Mongo::Grid::FSBucket::Stream::Write)
      end

      context 'when options are provided' do

        let(:stream) do
          described_class.get(fs, mode, chunk_size: 100)
        end

        it 'sets the options on the stream object' do
          expect(stream.options[:chunk_size]).to eq(100)
        end
      end
    end
  end
end