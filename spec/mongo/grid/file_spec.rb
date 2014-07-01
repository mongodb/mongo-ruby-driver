require 'spec_helper'

describe Mongo::Grid::File do

  let(:client)   { Mongo::Client.new(['localhost:27017'], :database => TEST_DB) }
  let(:database) { Mongo::Database.new(client, :test) }
  let(:files)    { collection(:fs_files, database) }
  let(:chunks)   { collection(:fs_chunks, database) }
  let(:filename) { "test-grid-file.txt" }
  let(:msg)      { "The rain in Spain falls mainly on the plains" }
  let(:id)       { BSON::ObjectId.new }
  let(:f_r)      { described_class.new(filename, 'r', files, chunks) }
  let(:f_w)      { described_class.new(filename, 'w', files, chunks) }

  let(:meta) do
    { :_id         => id,
      :filename    => filename,
      :length      => msg.length,
      :uploadDate  => Time.now.utc,
      :md5         => Digest::MD5.new,
      :contentType => 'text/plain',
      :aliases     => [],
      :chunkSize   => Mongo::Grid::DEFAULT_CHUNK_SIZE,
      :metadata    => {} }
  end

  let(:chunk) do
    { :n        => 0,
      :_id      => BSON::ObjectId.new,
      :files_id => meta[:_id],
      :data     => msg }
  end

  before do
    files.save(meta)
    chunks.save(chunk)
  end

  before :nofiles => true do
    chunks.remove({})
    files.remove({})
  end

  describe '#open' do

    context 'mode is neither w nor r' do

      it 'raises an error' do
        expect{ f = described_class.new(id, 'r+w', files, chunks) }.to raise_error
      end
    end

    context 'when mode is r' do

      context 'when file does not already exist', :nofiles do

        it 'creates' do
          expect{ described_class.new(filename, 'r', files, chunks) }.to raise_error
        end
      end

      context 'when file does exist' do

        context 'when id is a filename' do

          it 'opens the first found matching file' do
            expect(described_class.new(filename, 'r', files, chunks)).to be_a(described_class)
          end
        end
      end
    end

    context 'when mode is w' do

      context 'when id is a filename' do

        context 'when file does not exist', :nofiles do

          it 'creates a new file with this name' do
            f = described_class.new(filename, 'w', files, chunks)
            f.write(msg)
            expect(files.count({ :filename => filename })).to eq(1)
          end
        end

        context 'when file already exists' do

          it 'returns a reference to that file' do
            f_w2 = described_class.new(filename, 'w', files, chunks)
            expect(f_w2.files_id).to eq(id)
          end

          it 'truncates the existing file' do
            f_w2 = described_class.new(filename, 'w', files, chunks)
            expect(chunks.find_one({ :files_id => id })[:data]).to eq('')
          end
        end
      end

      context 'when id is an ObjectId' do

        context 'when file does not exist', :nofiles do

          it 'raises an error' do
            expect{ f = described_class.new(id, 'w', files, chunks) }.to raise_error
          end
        end

        context 'when file exists' do

          it 'returns a Grid::File object for that file' do
            f = described_class.new(id, 'w', files, chunks)
            expect(f.files_id).to eq(id)
          end
        end
      end

      context 'when options are passed', :nofiles do

        let(:custom_metadata) { { :type => "test" } }
        let(:custom_aliases) { [ "newfile.txt" ] }
        let(:custom_content) { 'text/plain' }
        let(:f_custom) { described_class.new(filename, 'w', files, chunks,
                                             { :chunk_size   => 5,
                                               :metadata     => custom_metadata,
                                               :content_type => custom_content,
                                               :aliases      => custom_aliases,
                                               :_id          => id }) }
        let(:files_doc) { files.find_one({ :filename => filename }) }

        before do
          f_custom.write(msg)
        end

        it 'sets a custom chunkSize' do
          expect(files_doc[:chunkSize]).to eq(5)
        end

        it 'sets custom metadata' do
          expect(files_doc[:metadata]).to eq(custom_metadata)
        end

        it 'sets custom aliases' do
          expect(files_doc[:aliases]).to eq(custom_aliases)
        end

        it 'sets a custom ObjectId' do
          expect(files_doc[:_id]).to eq(id)
        end

        it 'sets a custom contentType' do
          expect(files_doc[:contentType]).to eq(custom_content)
        end
      end
    end
  end

  describe '#size' do

    it 'returns an Integer' do
      expect(f_w.size).to be_a(Integer)
    end

    it 'returns the length of the file' do
      f_w.write(msg)
      expect(f_w.size).to eq(msg.length)
    end
  end

  describe '#read' do

    context 'when file is opened in w mode' do

      it 'raises an error' do
        expect{ f_w.read(10) }.to raise_error
      end
    end

    context 'when file is opened in r mode' do

      it 'returns a String' do
        expect(f_r.read(10)).to be_a(String)
      end

      it 'returns data from the file' do
        expect(f_r.read).to eq(msg)
      end
    end
  end

  describe '#write' do

    context 'when file is opened in w mode', :nofiles do

      it 'writes data to the file' do
        f_w.write(msg)
        expect(f_w.size).to eq(msg.length)
      end

      it 'returns the number of characters written' do
        expect(f_w.write(msg)).to eq(msg.length)
      end
    end

    context 'when file is opened in r mode' do

      it 'raises an error' do
        expect{ f_r.write(msg) }.to raise_error
      end
    end
  end

  describe '#==' do

    context 'when files_id and mode are the same' do

      let(:f_r2) { described_class.new(filename, 'r', files, chunks) }

      it 'returns true' do
        expect(f_r2 == f_r).to be(true)
      end
    end

    context 'when files_id are the same, but not mode' do

      it 'returns false' do
        expect(f_w == f_r).to be(false)
      end
    end

    context 'when modes are the same, but not files_id' do

      let(:f_w2) { described_class.new("test-file-2", 'w', files, chunks) }

      it 'returns false' do
        expect(f_w == f_w2).to be(false)
      end
    end
  end
end
