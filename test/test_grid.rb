require 'test/test_helper'

class GridTest < Test::Unit::TestCase

  def setup
    @db ||= Connection.new(ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost',
      ENV['MONGO_RUBY_DRIVER_PORT'] || Connection::DEFAULT_PORT).db('ruby-mongo-test')
    @files  = @db.collection('fs.files')
    @chunks = @db.collection('fs.chunks')
  end

  def teardown
    @files.remove
    @chunks.remove
  end

  context "When reading:" do
    setup do
      @data = "CHUNKS" * 50000
      @grid = Grid.new(@db)
      @grid.open('sample', 'w') do |f|
        f.write @data
      end

      @grid = Grid.new(@db)
    end

    should "read sample data" do
      data = @grid.open('sample', 'r') { |f| f.read }
      assert_equal data.length, @data.length
    end

    should "return an empty string if length is zero" do
      data = @grid.open('sample', 'r') { |f| f.read(0) }
      assert_equal '', data
    end

    should "return the first n bytes" do
      data = @grid.open('sample', 'r') {|f| f.read(288888) }
      assert_equal 288888, data.length
      assert_equal @data[0...288888], data
    end

    should "return the first n bytes even with an offset" do
      data = @grid.open('sample', 'r') do |f| 
        f.seek(1000)
        f.read(288888)
      end
      assert_equal 288888, data.length
      assert_equal @data[1000...289888], data
    end
  end

  context "When writing:" do
    setup do
      @data   = "BYTES" * 50000
      @grid = Grid.new(@db)
      @grid.open('sample', 'w') do |f|
        f.write @data
      end
    end

    should "read sample data" do
      data = @grid.open('sample', 'r') { |f| f.read }
      assert_equal data.length, @data.length
    end

    should "return the total number of bytes written" do
      data = 'a' * 300000
      assert_equal 300000, @grid.open('write', 'w') {|f| f.write(data) }
    end

    should "more read sample data" do
      data = @grid.open('sample', 'r') { |f| f.read }
      assert_equal data.length, @data.length
    end

    should "raise exception if not opened for write" do
      assert_raise GridError do
        @grid.open('io', 'r') { |f| f.write('hello') }
      end
    end
  end

  context "When appending:" do
    setup do
      @data   = "1"
      @grid = Grid.new(@db)
      @grid.open('sample', 'w', :chunk_size => 1000) do |f|
        f.write @data
      end
    end

    should "add data to the file" do
      new_data = "2"
      @grid.open('sample', 'w+') do |f|
        f.write(new_data)
      end

      all_data = @grid.open('sample', 'r') {|f| f.read }
      assert_equal @data + new_data, all_data
    end

    should "add multi-chunk-data" do
      new_data = "2" * 5000

      @grid.open('sample', 'w+') do |f|
        f.write(new_data)
      end

      all_data = @grid.open('sample', 'r') {|f| f.read }
      assert_equal @data + new_data, all_data
    end
  end

  context "When writing chunks:" do
    setup do
      data   = "B" * 50000
      @grid = Grid.new(@db)
      @grid.open('sample', 'w', :chunk_size => 1000) do |f|
        f.write data
      end
    end

    should "write the correct number of chunks" do
      file   = @files.find_one({:filename => 'sample'})
      chunks = @chunks.find({'files_id' => file['_id']}).to_a
      assert_equal 50, chunks.length
    end
  end

  context "Positioning:" do
    setup do
      data = 'hello, world' + '1' * 5000 + 'goodbye!' + '2' * 1000 + '!'
      @grid = Grid.new(@db)
      @grid.open('hello', 'w', :chunk_size => 1000) do |f|
        f.write data
      end
    end

    should "seek within chunks" do
      @grid.open('hello', 'r') do |f|
        f.seek(0)
        assert_equal 'h', f.read(1)
        f.seek(7)
        assert_equal 'w', f.read(1)
        f.seek(4)
        assert_equal 'o', f.read(1)
        f.seek(0)
        f.seek(7, IO::SEEK_CUR)
        assert_equal 'w', f.read(1)
        f.seek(-1, IO::SEEK_CUR)
        assert_equal ' ', f.read(1)
        f.seek(-4, IO::SEEK_CUR)
        assert_equal 'l', f.read(1)
        f.seek(3, IO::SEEK_CUR)
        assert_equal ',', f.read(1)
      end
    end

    should "seek between chunks" do
      @grid.open('hello', 'r') do |f|
        f.seek(1000)
        assert_equal '11111', f.read(5)

        f.seek(5009)
        assert_equal '111goodbye!222', f.read(14)

        f.seek(-1, IO::SEEK_END)
        assert_equal '!', f.read(1)
        f.seek(-6, IO::SEEK_END)
        assert_equal '2', f.read(1)
      end
    end

    should "tell the current position" do
      @grid.open('hello', 'r') do |f|
        assert_equal 0, f.tell

        f.seek(999)
        assert_equal 999, f.tell
      end
    end

    should "seek only in read mode" do
      assert_raise GridError do
        @grid.open('hello', 'w+') {|f| f.seek(0) }
      end
    end
  end
end
