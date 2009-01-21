$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'digest/md5'
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
    @users = @db.collection('system.users')
    @users.clear
    @db.add_user(@spongebob, @spongebob_password)
  end

  def teardown
    if @db.connected?
      @users.clear if @users
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
    @users = nil
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
    assert_equal 1, @users.count
    doc = @users.find({}, :limit => 1).next_object
    assert_equal @spongebob, doc['user']
    assert_equal Digest::MD5.hexdigest("mongo#{@spongebob_password}"), doc['pwd']
  end

  def test_delete_user
    @db.delete_user(@spongebob)
    assert_equal 0, @users.count
  end

  def test_authenticate
    assert !@db.authenticate('nobody', 'nopassword')
    assert !@db.authenticate(@spongebob, 'squareliederhosen')
    assert @db.authenticate(@spongebob, @spongebob_password)
  end

  def test_logout
    @db.logout                  # only testing that we don't throw exception
  end

end
