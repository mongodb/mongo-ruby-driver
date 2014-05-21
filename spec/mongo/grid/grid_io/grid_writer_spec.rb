require 'spec_helper'

describe Mongo::Grid::GridIO::GridWriter do
  include_context 'grid io helpers'

  describe '#initialize' do

    context 'when a String filename is used' do

      context 'when there is an existing file with that name' do

        it 'adds the new file alongside the old' do
          f.write(msg)
          f2 = described_class.new(files, chunks, filename)
          expect(files.count).to eq(2)
        end
      end

      context 'when there are no files with that name' do

        it 'adds the new file' do
          f.write(msg)
          expect(files.count).to eq(1)
        end
      end
    end

    context 'when an ObjectID is used' do

      context 'there is a matching file in the grid' do

        let(:f_id) { described_class.new(files, chunks, id) }

        before(:each) do
          files.save(meta)
          chunks.save(chunk)
          f_id.write(msg)
        end

        it 'opens the file' do
          expect(f_id.open?).to be(true)
        end

        it 'writes over the old entry for this file' do
          expect(files.count).to eq(1)
        end
      end

      context 'no file with this id exists' do

        it 'raises and error' do
          expect{described_class.new(files, chunks, BSON::ObjectId.new)}.to raise_error
        end
      end
    end

    context 'when options are given' do
      let(:custom_size) { 512 * 1024 }
      let(:custom_id) { BSON::ObjectId.new }
      let(:custom_type) { 'text/richtext' }
      let(:custom_aliases) { [ 'Test', 'testtest' ] }
      let(:f_opts) {
        described_class.new(files, chunks, filename,
                            { :chunk_size => custom_size,
                              :_id => custom_id,
                              :aliases => custom_aliases,
                              :metadata => { :value => 'custom' },
                              :content_type => custom_type,
                              :unique_filenames => true })
      }

      it 'sets chunk_size to specified value' do
        expect(f_opts.chunk_size).to eq(custom_size)
      end

      it 'sets files_id to custom id' do
        expect(f_opts.files_id).to eq(custom_id)
      end

      it 'adds a doc to the files collection with custom metadata' do
        pending 'collection implementation'
      end

      context 'when a valid content type is given' do

        it 'sets the content type' do
          expect(f_opts.files_doc['contentType']).to eq(custom_type)
        end
      end

      context 'when an invalid content type is given' do

        let(:bad_type) { "rainbow/unicorn" }
        let(:f_bad_type) { described_class.new(files,
                                               chunks,
                                               "test-with-opts",
                                               {:content_type => bad_type})}

        it 'does not use the invalid content type' do
          expect(f_bad_type.files_doc['contentType']).not_to eq(bad_type)
        end
      end

      context 'when aliases are given' do

        it 'sets custom aliases' do
          expect(f_opts.files_doc['aliases']).to eq(custom_aliases)
        end

        it 'overwrites the aliased files' do
          pending 'alias spec'
          # What could happen when we open a file "foo" with no aliases:
          #
          # A) No files exist with filename "foo", and no files list "foo"
          # as an alias.
          #   --> create new file.
          #
          # B) A file exists with filename "foo", and no files list "foo"
          # as an alias.
          #   --> create new file, overwrite old "foo", but save any
          #       aliases that file had.
          #
          # C) No file exists with filename "foo", but a file lists "foo"
          # as an alias.
          #   --> create a new file, overwrite file that aliased "foo", but
          #       save any aliases that file had?
          #
          # D) File exists with filename "foo", and that file lists "foo"
          # as an alias.
          #   --> this should never happen.
          #
          # E) File "foo" exists, and another file lists "foo" as an alias.
          #   --> this should never happen.
          #
          # F) No file "foo" exists, several files list "foo" as an alias.
          #   --> this should never happen.
          #

          # When we open a file "foo" with aliases ["bar", "boo"]:
          #
          # A) No files have filename "foo" or "bar" or "boo" or alias them.
          #   --> create the file.
          #
          # B) File "foo" exists:
          #   --> overwrite "foo", take its aliases.
          #
          # ...to be continued.
        end
      end
    end

    context 'when run without options' do

      let(:default) { 255 * 1024 }
      let(:f_default) { described_class.new(files, chunks, filename, {}) }

      it 'sets chunk_size to DEFAULT_CHUNK_SIZE' do
        expect(f_default.chunk_size).to eq(default)
      end

      it 'adds a doc to the files collection' do
        pending 'collection implementation'
      end

      it 'does not write over files with shared filenames' do
        f_prev = described_class.new(files, chunks, filename)
        f_prev.write(msg)
        f_prev.close
        f_default.write(msg)
        expect(files.find({ 'filename' => filename }).length).to eq(2)
      end

      context 'when filename includes content type' do

        it 'infers the content type from the filename' do
          expect(f.files_doc['contentType']).to eq('text/plain')
        end
      end

      context 'when filename does not include content type' do

        let(:f_no_type) { described_class.new(files, chunks, "rainbow.unicorn") }

        it 'uses the default content type' do
          expect(f_no_type.files_doc['contentType']).to eq('binary/octet-stream')
        end
      end
    end
  end

  describe '#write' do

    context 'on the first write' do

      it 'returns the number of characters written' do
        expect(f.write(msg)).to eq(msg.length)
      end

      it 'adds a document to the chunks collection' do
        pending 'collection implementation'
      end
    end

    context 'on subsequent writes' do

      before(:each) do
        f.write(msg)
      end

      it 'returns the number of bytes written' do
        expect(f.write(msg)).to eq(msg.length)
      end

      it 'length of file is the sum of both writes' do
        f.write(msg)
        expect(f.file_position).to eq(msg.length*2)
      end

      it 'appends the new message to the first in the chunks collection' do
        f.write(msg)
        pending 'collection implementation'
      end
    end

    context 'when passed an io object' do

      let(:io_w) { File.new(filename, "w") }
      let(:io_r) { File.new(filename, "r") }

      before(:each) do
        io_w.write(msg)
        io_w.close
      end

      it 'returns the number of bytes written' do
        expect(f.write(io_r)).to eq(msg.length)
      end

      it 'properly stores the message in the chunks collection' do
        pending 'collection implementation'
      end

      after(:each) do
        io_r.close
        File.delete(filename)
      end
    end

    context 'when write concern is gle' do
      pending 'collection implementation'
    end
  end

  describe '#open?' do

    context 'when the file is open' do

      it 'returns true' do
        expect(f.open?).to eq(true)
      end
    end

    context 'when the file is closed' do

      before(:each) { f.close }

      it 'returns false' do
        expect(f.open?).to eq(false)
      end
    end
  end

  describe '#inspect' do

    it 'returns a string' do
      expect(f.inspect).to be_a(String)
    end

    it 'returns a string containing the files_id' do
      expect(f.inspect).to match(/.*#{f.files_id}.*/)
    end

    it 'returns a string containing the filename' do
      expect(f.inspect).to match(/.*#{filename}.*/)
    end
  end
end
