shared_context 'grid' do
  let(:client) { Mongo::Client.new(['localhost:27017']) }
  let(:database) { Mongo::Database.new(client, :test) }
  # TODO db
  let(:files) { collection(:fs_files, database) }
  let(:chunks) { collection(:fs_chunks, database) }
  let(:filename) { "test-grid.txt" }
  let(:id) { BSON::ObjectId.new }
  let(:msg) { "The rain in Spain falls mainly on the plains" }

  let(:meta) do
    { '_id' => id,
      'filename' => filename,
      'length' => msg.length,
      'chunkSize' => 10,
      'uploadDate' => Time.now.utc,
      'md5' => Digest::MD5.new,
      'contentType' => 'text/plain',
      'aliases' => [],
      'metadata' => {} }
  end

  let(:chunk) do
    { 'n' => 0,
      '_id' => BSON::ObjectId.new,
      'files_id' => meta['_id'],
      'data' => msg }
  end
end

shared_context 'gridfs implementation' do
  include_context 'grid'

  let(:grid) do
    described_class.new(files, chunks)
  end

  let(:f_write) do
    grid.open(filename, "w")
  end

  before(:each) do
    files.save(meta)
    chunks.save(chunk)
  end
end

shared_context 'grid io helpers' do
  include_context 'grid'

  let(:f) do
    described_class.new(files, chunks, filename,
                        { :chunk_size => 10,
                          :unique_filenames => true })
  end
end

shared_examples 'a storable object' do

  describe '#open' do

    context 'when mode is "r"' do

      context 'when there is no such file' do

        it 'raises an error' do
          expect{grid.open("no such file", "r")}.to raise_error
        end
      end

      context 'when the file exists' do

        it 'returns a GridReader object' do
          expect(grid.open(filename, "r")).to be_a(Mongo::Grid::GridIO::GridReader)
        end

        it 'returns the correct GridReader object' do
          expect(grid.open(filename, "r").files_id).to eq(meta['_id'])
        end
      end
    end

    context 'when mode is "w"' do

      it 'returns a GridWriter object' do
        expect(grid.open(filename, "w")).to be_a(Mongo::Grid::GridIO::GridWriter)
      end
    end

    context 'when mode is neither "r" nor "w"' do

      it 'raises an error' do
        expect{grid.open(filename, "r+w")}.to raise_error
      end
    end
  end

  describe '#put' do

    before(:each) do
      grid.put("some data", "somenewfile.txt")
    end

    it 'returns a files_id' do
      expect(grid.put("some data", "newfile.txt")).to be_a(BSON::ObjectId)
    end

    it 'adds a new file to the system' do
      expect(grid.count).to eq(2)
    end

    it 'writes data to a new file' do
      files_doc = files.find_one({ 'filename' => "somenewfile.txt" })
      chunk = chunks.find_one({ 'files_id' => files_doc['_id'] })
      expect(chunk['data']).to eq("some data")
    end
  end

  describe '#get' do

    it 'returns a GridReader object' do
      expect(grid.get(filename)).to be_a(Mongo::Grid::GridIO::GridReader)
    end

    it 'raises an error if the file does not exist' do
      expect{grid.get("not a file")}.to raise_error
    end
  end

  describe '#count' do

    it 'returns an integer' do
      expect(grid.count).to be_a(Integer)
    end

    it 'returns the number of files in the system' do
      expect(grid.count).to eq(1)
    end
  end

  describe '#delete_all' do

    before(:each) do
      grid.delete_all
    end

    it 'removes all documents from the files collection' do
      expect(files.count).to eq(0)
    end

    it 'removes all chunks from the chunks collection' do
      expect(chunks.count).to eq(0)
    end
  end

  describe '#find' do

    context 'when there are matching files' do

      it 'returns an array of GridReader objects' do
        results = grid.find({ 'filename' => filename })
        expect(results[0]).to be_a(Mongo::Grid::GridIO::GridReader)
      end

      it 'returns only matches to this query' do
        results = grid.find({ 'filename' => filename })
        expect(results.length).to eq(1)
        expect(results[0].files_id).to eq(id)
      end
    end

    context 'when there are no matching files' do

      it 'returns an empty array' do
        expect(grid.find({ 'filename' => "not a filename" })).to eq([])
      end
    end
  end
end
