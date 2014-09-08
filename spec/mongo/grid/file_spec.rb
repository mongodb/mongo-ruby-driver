require 'spec_helper'

describe Mongo::Grid::File do

  let(:files)    { authorized_client[:fs_files] }
  let(:chunks)   { authorized_client[:fs_chunks] }
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
      :md5         => Digest::MD5.new.to_s,
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

  after do
    chunks.find.remove
    files.find.remove
  end

  describe '#open' do

    context 'mode is neither w nor r' do

      it 'raises an error' do
        expect {
          described_class.new(id, 'r+w', files, chunks)
        }.to raise_error
      end
    end

    context 'when mode is r (read)' do

      context 'when the file does not exist' do

        it 'raises an error' do
          expect {
            described_class.new(filename, 'r', files, chunks)
          }.to raise_error
        end
      end

      context 'when file does exist' do

        before do
          files.insert_one(meta)
          chunks.insert_one(chunk)
        end

        context 'when id is a filename' do

          let(:file) do
            described_class.new(filename, 'r', files, chunks)
          end

          it 'opens the first found matching file' do
            expect(file).to be_a(described_class)
          end
        end
      end
    end

    context 'when mode is w' do

      context 'when id is a filename' do

        pending 'when file does not exist' do

          let(:file) do
            described_class.new(filename, 'w', files, chunks)
          end

          before do
            file.write(msg)
          end

          it 'creates a new file with this name' do
            expect(files.find(:filename => filename).count).to eq(1)
          end
        end

        pending 'when file already exists' do

          let(:file) do
            described_class.new(filename, 'w', files, chunks)
          end

          it 'returns a reference to that file' do
            expect(file.files_id).to eq(id)
          end

          it 'truncates the existing file' do
            expect(chunks.find(:files_id => id).first[:data]).to eq('')
          end
        end
      end

      context 'when id is an ObjectId' do

        context 'when file does not exist' do

          it 'raises an error' do
            expect {
              described_class.new(id, 'w', files, chunks)
            }.to raise_error
          end
        end

        pending 'when file exists' do

          before do
            files.insert([ meta ])
            chunks.insert([ chunk ])
          end

          let(:file) do
            described_class.new(id, 'w', files, chunks)
          end

          it 'returns a Grid::File object for that file' do
            expect(file.files_id).to eq(id)
          end
        end
      end

      pending 'when options are passed' do

        let(:custom_metadata) { { :type => "test" } }
        let(:custom_aliases) { [ "newfile.txt" ] }
        let(:custom_content) { 'text/plain' }
        let(:f_custom) { described_class.new(filename, 'w', files, chunks,
                                             { :chunk_size   => 5,
                                               :metadata     => custom_metadata,
                                               :content_type => custom_content,
                                               :aliases      => custom_aliases,
                                               :_id          => id }) }
        let(:files_doc) { files.find(:filename => filename).first }

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

  pending '#size' do

    it 'returns an Integer' do
      expect(f_w.size).to be_a(Integer)
    end

    it 'returns the length of the file' do
      f_w.write(msg)
      expect(f_w.size).to eq(msg.length)
    end
  end

  pending '#read' do

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

  pending '#write' do

    context 'when file is opened in w mode' do

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

  pending '#==' do

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
