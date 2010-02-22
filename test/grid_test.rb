require 'test/test_helper'
include Mongo

class GridTest < Test::Unit::TestCase
  context "Tests:" do
    setup do
      @db ||= Connection.new(ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost',
        ENV['MONGO_RUBY_DRIVER_PORT'] || Connection::DEFAULT_PORT).db('ruby-mongo-test')
      @files  = @db.collection('test-fs.files')
      @chunks = @db.collection('test-fs.chunks')
    end

    teardown do
      @files.remove
      @chunks.remove
    end

    context "A basic grid-stored file" do
      setup do
        @data = "GRIDDATA" * 50000
        @grid = Grid.new(@db, 'test-fs')
        @id   = @grid.put(@data, 'sample', :metadata => {'app' => 'photos'})
      end

      should "retrieve the stored data" do
        data = @grid.get(@id).data
        assert_equal @data, data
      end

      should "store the filename" do
        file = @grid.get(@id)
        assert_equal 'sample', file.filename
      end

      should "store any relevant metadata" do
        file = @grid.get(@id)
        assert_equal 'photos', file.metadata['app']
      end

      should "delete the file and any chunks" do
        @grid.delete(@id)
        assert_raise GridError do
          @grid.get(@id)
        end
      end
    end

    context "Streaming: " do || {}
      setup do
        def read_and_write_stream(filename, read_length, opts={})
          io   = File.open(File.join(File.dirname(__FILE__), 'data', filename), 'r')
          id   = @grid.put(io, filename + read_length.to_s, opts)
          file = @grid.get(id)
          io.rewind
          data = io.read
          if data.respond_to?(:force_encoding)
            data.force_encoding(:binary)
          end
          read_data = ""
          while(chunk = file.read(read_length))
            read_data << chunk
          end
          assert_equal data.length, read_data.length
          assert_equal data, read_data, "Unequal!"
        end

        @grid = Grid.new(@db, 'test-fs')
      end

      should "put and get a small io object with a small chunk size" do
        read_and_write_stream('small_data.txt', 1, :chunk_size => 2)
      end

      should "put and get a small io object" do
        read_and_write_stream('small_data.txt', 1)
      end

      should "put and get a large io object when reading smaller than the chunk size" do
        read_and_write_stream('sample_file.pdf', 256 * 1024)
      end

      should "put and get a large io object when reading larger than the chunk size" do
        read_and_write_stream('sample_file.pdf', 300 * 1024)
      end
    end
  end
end
