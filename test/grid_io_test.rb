require 'test/test_helper'
include Mongo

class GridIOTest < Test::Unit::TestCase

  context "GridIO" do
    setup do
      @db ||= Connection.new(ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost',
        ENV['MONGO_RUBY_DRIVER_PORT'] || Connection::DEFAULT_PORT).db('ruby-mongo-test')
      @files  = @db.collection('fs.files')
      @chunks = @db.collection('fs.chunks')
    end

    teardown do
      @files.remove
      @chunks.remove
    end

    context "Options" do
      setup do
        @filename = 'test'
        @mode     = 'w'
      end

      should "set default 256k chunk size" do
        file = GridIO.new(@files, @chunks, @filename, @mode)
        assert_equal 256 * 1024, file.chunk_size
      end

      should "set chunk size" do
        file = GridIO.new(@files, @chunks, @filename, @mode, :chunk_size => 1000)
        assert_equal 1000, file.chunk_size
      end
    end
  end

end
