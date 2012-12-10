require 'test_helper'

class GridTest < Test::Unit::TestCase

  context "GridFS: " do
    setup do
      @client   = stub()
      @client.stubs(:write_concern).returns({})
      @client.stubs(:read).returns(:primary)
      @client.stubs(:tag_sets)
      @client.stubs(:acceptable_latency)
      @db     = DB.new("testing", @client)
      @files  = mock()
      @chunks = mock()

      @db.expects(:[]).with('fs.files').returns(@files)
      @db.expects(:[]).with('fs.chunks').returns(@chunks)
      @db.stubs(:safe)
      @db.stubs(:read).returns(:primary)
    end

    context "Grid classe with standard connections" do
      setup do
        @client.expects(:class).returns(MongoClient)
        @client.expects(:read_primary?).returns(true)
      end

      should "create indexes for Grid" do
        @chunks.expects(:create_index)
        Grid.new(@db)
      end

      should "create indexes for GridFileSystem" do
        @files.expects(:create_index)
        @chunks.expects(:create_index)
        GridFileSystem.new(@db)
      end
    end

    context "Grid classes with slave connection" do
      setup do
        @client.expects(:class).twice.returns(MongoClient)
        @client.expects(:read_primary?).returns(false)
      end

      should "not create indexes for Grid" do
        Grid.new(@db)
      end

      should "not create indexes for GridFileSystem" do
        GridFileSystem.new(@db)
      end
    end
  end
end
