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
    @users.insert(:user => @spongebob, :pwd => @db.send(:hash_password, @spongebob, @spongebob_password))
  end

  def teardown
    if @db && @db.connected?
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

  def test_pair
    @db.close
    @users = nil
    @db = Mongo.new({:left => "nosuch.example.com", :right => [@host, @port]}).db('ruby-mongo-test')
    assert @db.connected?
  end

  def test_pk_factory
    db = Mongo.new(@host, @port).db('ruby-mongo-test', :pk => TestPKFactory.new)
    coll = db.collection('test')
    coll.clear

    # new id gets added to returned object
    obj = coll.insert('name' => 'Fred', 'age' => 42)
    row = coll.find({'name' => 'Fred'}, :limit => 1).next_object
    oid = row['_id']
    assert_not_nil oid
    assert_equal obj, row

    oid = XGen::Mongo::Driver::ObjectID.new
    obj = coll.insert('_id' => oid, 'name' => 'Barney', 'age' => 41)
    row = coll.find({'name' => 'Barney'}, :limit => 1).next_object
    db_oid = row['_id']
    assert_equal oid, db_oid
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

  def test_authenticate
    assert !@db.authenticate('nobody', 'nopassword')
    assert !@db.authenticate(@spongebob, 'squareliederhosen')
    assert @db.authenticate(@spongebob, @spongebob_password)
  end

  def test_logout
    @db.logout                  # only testing that we don't throw exception
  end

  def test_auto_connect
    @db.close
    db = Mongo.new(@host, @port, :auto_reconnect => true).db('ruby-mongo-test')
    assert db.connected?
    assert db.auto_reconnect?
    db.close
    assert !db.connected?
    assert db.auto_reconnect?
    db.collection('test').insert('a' => 1)
    assert db.connected?
  end

  def test_error
    doc = @db.send(:db_command, :forceerror => 1)
    assert @db.error?
    err = @db.error
    assert_match /forced error/, err

    # ask again
    assert @db.error?
    err2 = @db.error
    assert_equal err, err2
  end

  def test_text_port_number
    db = DB.new('ruby-mongo-test', [[@host, @port.to_s]])
    # If there is no error, all is well
    db.collection('users').clear
  end

end
