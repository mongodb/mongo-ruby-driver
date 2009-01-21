$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'md5'
require 'mongo'
require 'test/unit'

class TestPKFactory
  def create_pk(row)
    row['_id'] ||= XGen::Mongo::Driver::ObjectID.new
    row
  end
end

# NOTE: assumes Mongo is running
class DBTest < Test::Unit::TestCase

  include XGen::Mongo::Driver

  def setup
    @host = ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost'
    @port = ENV['MONGO_RUBY_DRIVER_PORT'] || Mongo::DEFAULT_PORT
    @db = Mongo.new(@host, @port).db('ruby-mongo-test')
    @spongebob = 'spongebob'
    @spongebob_password = 'squarepants'
  end

  def teardown
    if @db.connected?
      @db.close
    end
  end

  def test_close
    @db.close
    assert !@db.connected?
    begin
      @db.collection('test').insert('a' => 1)
      fail "expected 'NilClass' exception"
    rescue => ex
      assert_match /NilClass/, ex.to_s
    end
  end

  def test_full_coll_name
    coll = @db.collection('test')
    assert_equal 'ruby-mongo-test.test', @db.full_coll_name(coll.name)
  end

  def test_master
    # Doesn't really test anything since we probably only have one database
    # during this test.
    @db.switch_to_master
    assert @db.connected?
  end

  def test_array
    @db.close
    @db = Mongo.new([["nosuch.example.com"], [@host, @port]]).db('ruby-mongo-test')
    assert @db.connected?
  end

  def test_pk_factory
    db = Mongo.new(@host, @port).db('ruby-mongo-test', :pk => TestPKFactory.new)
    coll = db.collection('test')
    coll.clear

    obj = coll.insert('name' => 'Fred', 'age' => 42)
    row = coll.find({'name' => 'Fred'}, :limit => 1).next_object
    assert_equal obj, row

    oid = XGen::Mongo::Driver::ObjectID.new
    obj = coll.insert('_id' => oid, 'name' => 'Barney', 'age' => 41)
    row = coll.find({'name' => 'Barney'}, :limit => 1).next_object
    assert_equal obj, row

    coll.clear
  end

  def test_pk_factory_reset
    @db.pk_factory = Object.new # first time
    begin
      @db.pk_factory = Object.new
      fail "error: expected exception"
    rescue => ex
      assert_match /can not change PK factory/, ex.to_s
    end
  end

  def test_add_user
    coll = @db.collection('system.users')
    coll.clear
    begin
      assert_equal 0, coll.count
      @db.add_user(@spongebob, @spongebob_password)
      assert_equal 1, coll.count
      doc = coll.find({}, :limit => 1).next_object
      assert_equal @spongebob, doc['user']
      assert_equal MD5.new("mongo#{@spongebob_password}").to_s, doc['pwd']
    ensure
      coll.clear
    end
  end

  def test_delete_user
    coll = @db.collection('system.users')
    coll.clear
    begin
      assert_equal 0, coll.count
      @db.add_user(@spongebob, @spongebob_password)
      assert_equal 1, coll.count
      @db.delete_user(@spongebob)
      assert_equal 0, coll.count
    ensure
      coll.clear
    end
  end

  def test_authenticate
    coll = @db.collection('system.users')
    coll.clear
    begin
      @db.add_user(@spongebob, @spongebob_password)
      assert !@db.authenticate('nobody', 'nopassword')
      assert !@db.authenticate(@spongebob, 'squareliederhosen')
      assert @db.authenticate(@spongebob, @spongebob_password)
    ensure
      coll.clear
    end
  end

end
