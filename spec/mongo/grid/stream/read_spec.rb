require 'spec_helper'

describe Mongo::Grid::FSBucket::Stream::Read do

  let(:fs_options) do
    { read: { mode: :secondary } }
  end

  let(:fs) do
    authorized_client.database.fs(fs_options)
  end

  let(:options) do
    { file_id: file_id }
  end

  let(:filename) do
    'specs.rb'
  end

  let!(:file_id) do
    fs.upload_from_stream(filename, File.open(__FILE__))
  end

  after do
    fs.files_collection.delete_many
    fs.chunks_collection.delete_many
  end

  let(:stream) do
    described_class.new(fs, options)
  end

  describe '#initialize' do

    it 'sets the file id' do
      expect(stream.file_id).to eq(file_id)
    end

    it 'sets the fs object' do
      expect(stream.fs).to eq(fs)
    end

    it 'uses the read preference of the fs as a default' do
      expect(stream.read_preference).to eq(fs.read_preference)
    end

    it 'opens a stream' do
      expect(stream.close).to eq(file_id)
    end

    context 'when provided options' do

      context 'when provided read preference' do

        let(:options) do
          {
              file_id: file_id,
              read: { mode: :primary_preferred }
          }
        end

        it 'sets the read preference' do
          expect(stream.read_preference).to eq(Mongo::ServerSelector.get(options[:read]))
        end

        it 'sets the read preference on the view' do
          expect(stream.send(:view).read).to eq(Mongo::ServerSelector.get(options[:read]))
        end
      end

      context 'when provided a file_id' do

        it 'sets the file id' do
          expect(stream.file_id).to eq(options[:file_id])
        end
      end
    end
  end

  describe '#each' do

    let(:filename) do
      'specs.rb'
    end

    let!(:file_id) do
      fs.upload_from_stream(filename, File.open(__FILE__))
    end

    after do
      fs.files_collection.delete_many
      fs.chunks_collection.delete_many
    end

    let(:fs_options) do
      { chunk_size: 5 }
    end

    it 'iterates over all the chunks of the file' do
      stream.each do |chunk|
        expect(chunk).not_to be(nil)
      end
    end

    context 'when the stream is closed' do

      before do
        stream.close
      end

      it 'does not allow further iteration' do
        expect {
          stream.to_a
        }.to raise_error(Mongo::Error::ClosedStream)
      end
    end

    context 'when a chunk is found out of order' do

      before do
        view = stream.fs.chunks_collection.find({ :files_id => file_id }, options).sort(:n => -1)
        stream.instance_variable_set(:@view, view)
        expect(stream).to receive(:close)
      end

      it 'raises an exception' do
        expect {
          stream.to_a
        }.to raise_error(Mongo::Error::UnexpectedChunkN)
      end

      it 'closes the query' do
        begin
          stream.to_a
        rescue Mongo::Error::UnexpectedChunkN
        end
      end
    end

    context 'when there is no files document found' do

      before do
        fs.files_collection.delete_many
      end

      it 'raises an Exception' do
        expect{
          stream.to_a
        }.to raise_exception(Mongo::Error::NoFileInfo)
      end
    end
  end

  describe '#file_info' do

    it 'returns a files information document' do
      expect(stream.file_info).not_to be_nil
    end
  end

  describe '#close' do

    let(:view) do
      stream.instance_variable_get(:@view)
    end

    before do
      stream.to_a
    end

    it 'returns the file id' do
      expect(stream.close).to eq(file_id)
    end

    context 'when the stream is closed' do

      before do
        stream.to_a
      end

      it 'calls close_query on the view' do
        expect(view).to receive(:close_query).and_call_original
        stream.close
      end
    end

    context 'when the stream is already closed' do

      before do
        stream.close
      end

      it 'raises an exception' do
        expect {
          stream.close
        }.to raise_error(Mongo::Error::ClosedStream)
      end
    end
  end
end