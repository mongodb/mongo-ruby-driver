require 'spec_helper'

describe Mongo::Grid::GridIO::GridReader do
  include_context 'grid io helpers'

  let(:f_write) { Mongo::Grid::GridIO::GridWriter.new(files, chunks, filename) }

  before(:each) do
    f_write.write(msg)
    f_write.close
  end

  describe '#initialize' do

    context 'when "filename" is a String' do

      context 'when file "filename" exists' do

        it 'opens the file' do
          expect(f.open?).to eq(true)
        end
      end

      context 'when "filename" is an alias for an existing file' do

        it 'opens the aliased existing file' do
          pending 'alias spec'
        end
      end

      context 'when file "filename" does not exist and is not an alias' do

        it 'raises an error' do
          pending 'alias spec'
        end
      end
    end

    context 'when "filename" is an ObjectId' do

      context 'when file with this files_id exists' do

        let(:f_id) { described_class.new(files, chunks, f_write.files_id) }

        it 'opens the matching file' do
          expect(f_id.open?).to eq(true)
        end
      end

      context 'when there is no file with this files_id' do

        it 'raises an error' do
          expect{f_none = described_class.new(files, chunks, BSON::ObjectId.new)}.to raise_error(Mongo::GridError)
        end
      end
    end
  end

  describe '#read' do

    context 'when length is not specified' do

      it 'reads from current file position to eof' do
        f.seek(10)
        expect(f.read).to eq(msg[10, msg.length - 10])
      end
    end

    context 'when length is specified' do

      it 'reads specified number of characters' do
        expect(f.read(8).length).to eq(8)
      end
    end

    context 'when the file is closed' do

      it 'raises an error' do
        f.close
        expect{f.read(8)}.to raise_error
      end
    end
  end

  describe '#each' do

    context 'when passed a block' do
      # TODO
    end

    context 'when no block is given' do

      it 'returns an enumerator' do
        # TODO
      end
    end

    context 'when the file is closed' do

      it 'raises an error' do
        f.close
        expect{f.each}.to raise_error
      end
    end
  end

  describe '#read_all' do

    it 'returns the entire file contents' do
      # TODO db
      # read a long message.  Or set a very small chunk size.
      expect(f.read_all).to eq(msg)
    end

    it 'puts the file pointer at eof' do
      s = f.read_all
      expect(f.eof?).to eq(true)
    end

    context 'when the file is closed' do

      it 'raises an error' do
        f.close
        expect{f.read_all}.to raise_error
      end
    end
  end

  describe '#read_to_character' do

    context 'when character is present' do

      it 'returns a string' do
        expect(f.read_to_character('a')).to be_a(String)
      end

      it 'reads until the first occurrence of character' do
        expect(f.read_to_character('a')).to eq("The ra")
      end
    end

    context 'when there is no match' do

      it 'returns the entire file' do
        expect(f.read_to_character('!')).to eq(msg)
      end
    end

    context 'when the file is closed' do

      it 'raises an error' do
        f.close
        expect{f.read_to_character('@')}.to raise_error
      end
    end
  end

  describe '#read_to_string' do

    context 'when substring is present' do

      it 'returns a string' do
        expect(f.read_to_string("ain")).to be_a(String)
      end

      it 'reads until the first occurrence of the string' do
        expect(f.read_to_string(msg[10, 4])).to eq(msg[0, 14])
      end
    end

    context 'when there is no match' do

      it 'returns the entire file' do
        expect(f.read_to_string("ainain")).to eq(msg)
      end
    end

    context 'when the file is closed' do

      it 'raises an error' do
        f.close
        expect{f.read_to_string("cat")}.to raise_error
      end
    end
  end

  describe '#seek' do

    context 'when whence is IO::SEEK_CUR' do

      it 'returns the old file position + n' do
        old_position = f.file_position
        expect(f.seek(10, IO::SEEK_CUR)).to eq(old_position + 10)
      end

      it 'sets the file position to the old position + n' do
        old_position = f.file_position
        f.seek(10, IO::SEEK_CUR)
        expect(f.file_position).to eq(old_position + 10)
      end
    end

    context 'when whence is IO::SEEK_SET' do

      it 'returns n' do
        expect(f.seek(10)).to eq(10)
      end

      it 'sets the file position to n' do
        f.seek(8)
        expect(f.file_position).to eq(8)
      end
    end

    context 'when whence is IO::SEEK_END' do

      it 'returns file_length + n' do
        expect(f.seek(10, IO::SEEK_END)).to eq(f.file_length + 10)
      end

      it 'sets the file position to file_length + n' do
        f.seek(10, IO::SEEK_END)
        expect(f.file_position).to eq(f.file_length + 10)
      end
    end

    context 'when the file is closed' do

      it 'raises an error' do
        f.close
        expect{f.seek(8)}.to raise_error
      end
    end
  end

  describe '#rewind' do

    it 'returns 0' do
      expect(f.rewind).to eq(0)
    end

    it 'resets the file position to 0' do
      f.rewind
      expect(f.file_position).to eq(0)
    end

    context 'when the file is closed' do

      it 'raises an error' do
        f.close
        expect{f.rewind}.to raise_error
      end
    end
  end

  describe '#tell' do

    it 'returns the current file position' do
      expect(f.tell).to eq(f.file_position)
    end

    context 'when the file is closed' do

      it 'raises an error' do
        f.close
        expect{f.tell}.to raise_error
      end
    end
  end

  describe '#eof?' do

    context 'when file is at eof' do

      it 'returns true' do
        f.seek(f.file_length + 10)
        expect(f.eof?).to eq(true)
      end
    end

    context 'when file is not at eof' do

      it 'returns false' do
        f.seek(0)
        expect(f.eof?).to eq(false)
      end
    end

    context 'when the file is closed' do

      it 'raises an error' do
        f.close
        expect{f.eof?}.to raise_error
      end
    end
  end

  describe "#open?" do

    context 'when the file is open' do

      it 'returns true' do
        expect(f.open?).to eq(true)
      end
    end

    context 'when the file is closed' do

      it 'returns false' do
        f.close
        expect(f.open?).to eq(false)
      end
    end
  end

  describe '#getc' do

    it 'returns the next character' do
      f.seek(10)
      expect(f.getc).to eq(msg[10])
    end

    context 'when the file is closed' do

      it 'raises an error' do
        f.close
        expect{f.getc}.to raise_error
      end
    end
  end

  describe '#gets' do
    let(:lines_file) { "test-grid-reader-with-lines.txt" }
    let(:line_one) { "Hickory dickory dock,\n" }
    let(:line_two) { "The mouse ran up the clock.\n" }

    let(:f_write_lines) { Mongo::Grid::GridIO::GridWriter.new(files, chunks, lines_file) }
    let(:f_lines) { described_class.new(files, chunks, lines_file) }

    before(:each) do
      f_write_lines.write(line_one)
      f_write_lines.write(line_two)
      f_write_lines.close
    end

    context 'on the first call' do

      it 'returns the first line' do
        pending 'collection implementation'
        #expect(f_lines.gets).to eq(line_one)
      end
    end

    context 'on the next call' do

      it 'returns the next line' do
        first_line = f_lines.gets
        pending 'collection implementation'
        #expect(f_lines.gets).to eq(line_two)
      end
    end

    context 'when there are no line breaks' do

      it 'returns all of the data' do
        expect(f.gets).to eq(msg)
      end
    end

    context 'at eof' do

      it 'returns nil' do
        f_lines.seek(f_lines.file_length)
        expect(f_lines.gets).to eq(nil)
      end
    end

    context 'when the file is closed' do

      it 'raises an error' do
        f.close
        expect{f.gets}.to raise_error
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
