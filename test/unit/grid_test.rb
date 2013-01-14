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

      @db.stubs(:[]).with('fs.files').returns(@files)
      @db.stubs(:[]).with('fs.chunks').returns(@chunks)
      @db.stubs(:safe)
      @db.stubs(:read).returns(:primary)
    end

    context "Grid classes with standard connections" do
      setup do
        @chunks.expects(:ensure_index)
      end

      should "create indexes for Grid" do
        Grid.new(@db)
      end

      should "create indexes for GridFileSystem" do
        @files.expects(:ensure_index)
        GridFileSystem.new(@db)
      end
    end

    context "Grid classes with slave connection" do
      setup do
        @chunks.stubs(:ensure_index).raises(Mongo::ConnectionFailure)
        @files.stubs(:ensure_index).raises(Mongo::ConnectionFailure)
      end

      should "not create indexes for Grid" do
        grid = Grid.new(@db)
        data = "hello world!"
        assert_raise Mongo::ConnectionFailure do
          grid.put(data)
        end
      end

      should "not create indexes for GridFileSystem" do
        gridfs = GridFileSystem.new(@db)
        data = "hello world!"
        assert_raise Mongo::ConnectionFailure do
          gridfs.open('image.jpg', 'w') do |f|
            f.write data
          end
        end
      end
    end
  end
end