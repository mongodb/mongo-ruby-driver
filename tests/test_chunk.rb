$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'test/unit'
require 'rubygems'
require 'mongo'
require 'mongo/gridfs'

class ChunkTest < Test::Unit::TestCase

  include XGen::Mongo::Driver
  include XGen::Mongo::GridFS

  def setup
    @host = ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost'
    @port = ENV['MONGO_RUBY_DRIVER_PORT'] || Mongo::DEFAULT_PORT
    @db = Mongo.new(@host, @port).db('ruby-mongo-utils-test')

    @files = @db.collection('_files')
    @chunks = @db.collection('_chunks')
    @chunks.clear
    @files.clear

    @c = Chunk.new(@chunks)
  end

  def teardown
    if @db && @db.connected?
      @chunks.clear
      @files.clear
      @db.close
    end
  end

  def test_has_next
    assert !@c.has_next?
    @c.next = Chunk.new(@chunks)
    assert @c.has_next?
  end

  def test_assign_next
    assert !@c.has_next?
    assert_nil @c.next

    c2 = Chunk.new(@chunks)
    @c.next = c2
    assert_same c2, @c.next
  end

  def test_pos
    assert_equal 0, @c.pos
    assert @c.eof?              # since data is empty

    b = ByteBuffer.new
    3.times { |i| b.put(i) }
    c = Chunk.new(@db, 'data' => b)
    assert !c.eof?
  end

  def test_getc
    b = ByteBuffer.new
    3.times { |i| b.put(i) }
    c = Chunk.new(@chunks, 'data' => b)

    assert !c.eof?
    assert_equal 0, c.getc
    assert !c.eof?
    assert_equal 1, c.getc
    assert !c.eof?
    assert_equal 2, c.getc
    assert c.eof?
  end

  def test_putc
    3.times { |i| @c.putc(i) }
    @c.pos = 0

    assert !@c.eof?
    assert_equal 0, @c.getc
    assert !@c.eof?
    assert_equal 1, @c.getc
    assert !@c.eof?
    assert_equal 2, @c.getc
    assert @c.eof?
  end

  def test_empty
    assert @c.empty?
    @c.putc(1)
    assert !@c.empty?
  end

  def test_truncate
    10.times { |i| @c.putc(i) }
    assert_equal 10, @c.size
    @c.pos = 3
    @c.truncate
    assert_equal 3, @c.size

    @c.pos = 0
    assert !@c.eof?
    assert_equal 0, @c.getc
    assert !@c.eof?
    assert_equal 1, @c.getc
    assert !@c.eof?
    assert_equal 2, @c.getc
    assert @c.eof?
  end

end
