require 'test/test_helper'

class GridTest < Test::Unit::TestCase

  def setup
    @db ||= Connection.new(ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost',
      ENV['MONGO_RUBY_DRIVER_PORT'] || Connection::DEFAULT_PORT).db('ruby-mongo-test')
    @files  = @db.collection('test-bucket.files')
    @chunks = @db.collection('test-bucket.chunks')
  end

  def teardown
    @files.remove
    @chunks.remove
  end

  context "A basic grid-stored file" do
    setup do
      @data = "GRIDDATA" * 50000
      @grid = Grid.new(@db, 'test-bucket')
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

end
