$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'test/unit'
require 'mongo'
require 'mongo/gridfs'

class ChunkTest < Test::Unit::TestCase

  include XGen::Mongo::Driver
  include XGen::Mongo::GridFS

  @@db = Mongo.new(ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost',
                   ENV['MONGO_RUBY_DRIVER_PORT'] || Mongo::DEFAULT_PORT).db('ruby-mongo-utils-test')
  @@files = @@db.collection('gridfs.files')
  @@chunks = @@db.collection('gridfs.chunks')

  def setup
    @@chunks.clear
    @@files.clear

    @f = GridStore.new(@@db, 'foobar', 'w')
    @c = @f.instance_variable_get('@curr_chunk')
  end

  def teardown
    @@chunks.clear
    @@files.clear
    @@db.error
  end

  def test_pos
    assert_equal 0, @c.pos
    assert @c.eof?              # since data is empty

    b = ByteBuffer.new
    3.times { |i| b.put(i) }
    c = Chunk.new(@f, 'data' => b)
    assert !c.eof?
  end

  def test_getc
    b = ByteBuffer.new
    3.times { |i| b.put(i) }
    c = Chunk.new(@f, 'data' => b)

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
