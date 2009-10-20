$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'digest/md5'
require 'mongo'
require 'test/unit'
require 'stringio'
require 'logger'

class TestPKFactory
  def create_pk(row)
    row['_id'] ||= Mongo::ObjectID.new
    row
  end
end

# NOTE: assumes Mongo is running
class DBTest < Test::Unit::TestCase

  include Mongo

  @@host = ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost'
  @@port = ENV['MONGO_RUBY_DRIVER_PORT'] || Connection::DEFAULT_PORT
  @@db = Connection.new(@@host, @@port).db('ruby-mongo-test')
  @@users = @@db.collection('system.users')

  def setup
    @spongebob = 'spongebob'
    @spongebob_password = 'squarepants'
    @@users.remove
    @@users.insert(:user => @spongebob, :pwd => @@db.send(:hash_password, @spongebob, @spongebob_password))
  end

  def teardown
    @@users.remove if @@users
    @@db.error
  end

  def test_close
    @@db.close
    assert !@@db.connected?
    begin
      @@db.collection('test').insert('a' => 1)
      fail "expected 'NilClass' exception"
    rescue => ex
      assert_match /NilClass/, ex.to_s
    ensure
      @@db = Connection.new(@@host, @@port).db('ruby-mongo-test')
      @@users = @@db.collection('system.users')
    end
  end
  
  def test_logger
    output = StringIO.new
    logger = Logger.new(output)
    logger.level = Logger::DEBUG
    db = Connection.new(@host, @port, :logger => logger).db('ruby-mongo-test')
    assert_equal logger, db.logger
    
    db.logger.debug 'testing'
    assert output.string.include?('testing')
  end

  def test_full_coll_name
    coll = @@db.collection('test')
    assert_equal 'ruby-mongo-test.test', @@db.full_collection_name(coll.name)
  end

  def test_collection_names
    @@db.collection("test").insert("foo" => 5)
    @@db.collection("test.mike").insert("bar" => 0)

    colls = @@db.collection_names()
    assert colls.include?("test")
    assert colls.include?("test.mike")
    colls.each { |name|
      assert !name.include?("$")
    }
  end

  def test_collections
    @@db.collection("test.durran").insert("foo" => 5)
    @@db.collection("test.les").insert("bar" => 0)

    colls = @@db.collections()
    assert_not_nil colls.select { |coll| coll.name == "test.durran" }
    assert_not_nil colls.select { |coll| coll.name == "test.les" }
    assert_equal [], colls.select { |coll| coll.name == "does_not_exist" }

    assert_kind_of Collection, colls[0]
  end

  def test_pair
    @@db.close
    @@users = nil
    @@db = Connection.new({:left => "this-should-fail", :right => [@@host, @@port]}).db('ruby-mongo-test')
    assert @@db.connected?
  ensure
    @@db = Connection.new(@@host, @@port).db('ruby-mongo-test') unless @@db.connected?
    @@users = @@db.collection('system.users')
  end

  def test_pk_factory
    db = Connection.new(@@host, @@port).db('ruby-mongo-test', :pk => TestPKFactory.new)
    coll = db.collection('test')
    coll.remove

    insert_id = coll.insert('name' => 'Fred', 'age' => 42)
    # new id gets added to returned object
    row = coll.find_one({'name' => 'Fred'})
    oid = row['_id']
    assert_not_nil oid
    assert_equal insert_id, oid

    oid = ObjectID.new
    data = {'_id' => oid, 'name' => 'Barney', 'age' => 41}
    coll.insert(data)
    row = coll.find_one({'name' => data['name']})
    db_oid = row['_id']
    assert_equal oid, db_oid
    assert_equal data, row

    coll.remove
  end

  def test_pk_factory_reset
    db = Connection.new(@@host, @@port).db('ruby-mongo-test')
    db.pk_factory = Object.new # first time
    begin
      db.pk_factory = Object.new
      fail "error: expected exception"
    rescue => ex
      assert_match /can not change PK factory/, ex.to_s
    ensure
      db.close
    end
  end

  def test_authenticate
    assert !@@db.authenticate('nobody', 'nopassword')
    assert !@@db.authenticate(@spongebob, 'squareliederhosen')
    assert @@db.authenticate(@spongebob, @spongebob_password)
  end

  def test_logout
    @@db.logout                  # only testing that we don't throw exception
  end

  def test_auto_connect
    @@db.close
    db = Connection.new(@@host, @@port, :auto_reconnect => true).db('ruby-mongo-test')
    assert db.connected?
    assert db.auto_reconnect?
    db.close
    assert !db.connected?
    assert db.auto_reconnect?
    db.collection('test').insert('a' => 1)
    assert db.connected?
  ensure
    @@db = Connection.new(@@host, @@port).db('ruby-mongo-test')
    @@users = @@db.collection('system.users')
  end

  def test_error
    @@db.reset_error_history
    assert_nil @@db.error
    assert !@@db.error?
    assert_nil @@db.previous_error

    @@db.send(:db_command, :forceerror => 1)
    assert @@db.error?
    assert_not_nil @@db.error
    assert_not_nil @@db.previous_error

    @@db.send(:db_command, :forceerror => 1)
    assert @@db.error?
    assert @@db.error
    prev_error = @@db.previous_error
    assert_equal 1, prev_error['nPrev']
    assert_equal prev_error["err"], @@db.error

    @@db.collection('test').find_one
    assert_nil @@db.error
    assert !@@db.error?
    assert @@db.previous_error
    assert_equal 2, @@db.previous_error['nPrev']

    @@db.reset_error_history
    assert_nil @@db.error
    assert !@@db.error?
    assert_nil @@db.previous_error
  end

  def test_last_status
    @@db['test'].remove
    @@db['test'].save("i" => 1)

    @@db['test'].update({"i" => 1}, {"$set" => {"i" => 2}})
    assert @@db.last_status()["updatedExisting"]

    @@db['test'].update({"i" => 1}, {"$set" => {"i" => 500}})
    assert !@@db.last_status()["updatedExisting"]
  end

  def test_text_port_number
    db = DB.new('ruby-mongo-test', [[@@host, @@port.to_s]])
    # If there is no error, all is well
    db.collection('users').remove
  end

end
