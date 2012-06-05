require File.expand_path("../test_helper", __FILE__)
require 'logger'
require 'stringio'
require 'thread'

class TestConnection < Test::Unit::TestCase

  include Mongo
  include BSON

  def setup
    @conn = standard_connection
  end

  def teardown
    @conn.close
  end

  def test_connection_failure
    assert_raise Mongo::ConnectionFailure do
      Mongo::Connection.new('localhost', 27347)
    end
  end

 # def test_connection_timeout
 #   passed = false
 #   begin
 #     t0 = Time.now
 #     Mongo::Connection.new('foo.bar', 27017, :connect_timeout => 3)
 #   rescue OperationTimeout
 #     passed = true
 #     t1 = Time.now
 #   end

 #   assert passed
 #   assert t1 - t0 < 4
 # end

  def test_host_port_accessors
    assert_equal @conn.host, TEST_HOST
    assert_equal @conn.port, TEST_PORT
  end

  def test_server_info
    server_info = @conn.server_info
    assert server_info.keys.include?("version")
    assert Mongo::Support.ok?(server_info)
  end

  def test_ping
    ping = @conn.ping
    assert ping['ok']
  end

  def test_connection_uri
    con = Connection.from_uri("mongodb://#{host_port}")
    assert_equal mongo_host, con.primary_pool.host
    assert_equal mongo_port, con.primary_pool.port
  end

  def test_env_mongodb_uri
    begin
      old_mongodb_uri = ENV['MONGODB_URI']
      ENV['MONGODB_URI'] = "mongodb://#{host_port}"
      con = Connection.new
      assert_equal mongo_host, con.primary_pool.host
      assert_equal mongo_port, con.primary_pool.port
    ensure
      ENV['MONGODB_URI'] = old_mongodb_uri
    end
  end

  def test_from_uri_implicit_mongodb_uri
    begin
      old_mongodb_uri = ENV['MONGODB_URI']
      ENV['MONGODB_URI'] = "mongodb://#{host_port}"
      con = Connection.from_uri
      assert_equal mongo_host, con.primary_pool.host
      assert_equal mongo_port, con.primary_pool.port
    ensure
      ENV['MONGODB_URI'] = old_mongodb_uri
    end
  end

  def test_server_version
    assert_match(/\d\.\d+(\.\d+)?/, @conn.server_version.to_s)
  end

  def test_invalid_database_names
    assert_raise TypeError do @conn.db(4) end

    assert_raise Mongo::InvalidNSName do @conn.db('') end
    assert_raise Mongo::InvalidNSName do @conn.db('te$t') end
    assert_raise Mongo::InvalidNSName do @conn.db('te.t') end
    assert_raise Mongo::InvalidNSName do @conn.db('te\\t') end
    assert_raise Mongo::InvalidNSName do @conn.db('te/t') end
    assert_raise Mongo::InvalidNSName do @conn.db('te st') end
  end

  def test_options_passed_to_db
    @pk_mock = Object.new
    db = @conn.db('test', :pk => @pk_mock, :strict => true)
    assert_equal @pk_mock, db.pk_factory
    assert db.strict?
  end

  def test_database_info
    @conn.drop_database(MONGO_TEST_DB)
    @conn.db(MONGO_TEST_DB).collection('info-test').insert('a' => 1)

    info = @conn.database_info
    assert_not_nil info
    assert_kind_of Hash, info
    assert_not_nil info[MONGO_TEST_DB]
    assert info[MONGO_TEST_DB] > 0

    @conn.drop_database(MONGO_TEST_DB)
  end

  def test_copy_database
    @conn.db('old').collection('copy-test').insert('a' => 1)
    @conn.copy_database('old', 'new', host_port)
    old_object = @conn.db('old').collection('copy-test').find.next_document
    new_object = @conn.db('new').collection('copy-test').find.next_document
    assert_equal old_object, new_object
    @conn.drop_database('old')
    @conn.drop_database('new')
  end

  def test_copy_database_with_auth
    @conn.db('old').collection('copy-test').insert('a' => 1)
    @conn.db('old').add_user('bob', 'secret')

    assert_raise Mongo::OperationFailure do
      @conn.copy_database('old', 'new', host_port, 'bob', 'badpassword')
    end

    result = @conn.copy_database('old', 'new', host_port, 'bob', 'secret')
    assert Mongo::Support.ok?(result)

    @conn.drop_database('old')
    @conn.drop_database('new')
  end

  def test_database_names
    @conn.drop_database(MONGO_TEST_DB)
    @conn.db(MONGO_TEST_DB).collection('info-test').insert('a' => 1)

    names = @conn.database_names
    assert_not_nil names
    assert_kind_of Array, names
    assert names.length >= 1
    assert names.include?(MONGO_TEST_DB)
  end

  def test_logging
    output = StringIO.new
    logger = Logger.new(output)
    logger.level = Logger::DEBUG
    standard_connection(:logger => logger).db(MONGO_TEST_DB)
    assert output.string.include?("admin['$cmd'].find")
  end

  def test_logging_duration
    output = StringIO.new
    logger = Logger.new(output)
    logger.level = Logger::DEBUG
    standard_connection(:logger => logger).db(MONGO_TEST_DB)
    assert_match(/\(\d+ms\)/, output.string)
    assert output.string.include?("admin['$cmd'].find")
  end

  def test_connection_logger
    output = StringIO.new
    logger = Logger.new(output)
    logger.level = Logger::DEBUG
    connection = standard_connection(:logger => logger)
    assert_equal logger, connection.logger

    connection.logger.debug 'testing'
    assert output.string.include?('testing')
  end

  def test_drop_database
    db = @conn.db('ruby-mongo-will-be-deleted')
    coll = db.collection('temp')
    coll.remove
    coll.insert(:name => 'temp')
    assert_equal 1, coll.count()
    assert @conn.database_names.include?('ruby-mongo-will-be-deleted')

    @conn.drop_database('ruby-mongo-will-be-deleted')
    assert !@conn.database_names.include?('ruby-mongo-will-be-deleted')
  end

  def test_nodes
    silently do
      @conn = Connection.multi([['foo', 27017], ['bar', 27018]], :connect => false)
    end
    seeds = @conn.seeds
    assert_equal 2, seeds.length
    assert_equal ['foo', 27017], seeds[0]
    assert_equal ['bar', 27018], seeds[1]
  end

  def test_fsync_lock
    assert !@conn.locked?
    @conn.lock!
    assert @conn.locked?
    assert_equal 1, @conn['admin']['$cmd.sys.inprog'].find_one['fsyncLock'], "Not fsync-locked"
    assert_match(/unlock/, @conn.unlock!['info'])
    unlocked = false
    counter  = 0
    while counter < 5
      if @conn['admin']['$cmd.sys.inprog'].find_one['fsyncLock'].nil?
        unlocked = true
        break
      else
        sleep(1)
        counter += 1
      end
    end
    assert !@conn.locked?
    assert unlocked, "mongod failed to unlock"
  end

  def test_max_bson_size_value
    conn = standard_connection(:connect => false)

    admin_db = Object.new
    admin_db.expects(:command).returns({'ok' => 1, 'ismaster' => 1, 'maxBsonObjectSize' => 15_000_000})
    conn.expects(:[]).with('admin').returns(admin_db)
    conn.connect
    assert_equal 15_000_000, conn.max_bson_size

    conn = standard_connection
    if conn.server_version > "1.7.2"
      assert_equal conn['admin'].command({:ismaster => 1})['maxBsonObjectSize'], conn.max_bson_size
    end

    conn.connect
    doc = {'n' => 'a' * (conn.max_bson_size)}
    assert_raise InvalidDocument do
      assert BSON::BSON_CODER.serialize(doc, false, true, @conn.max_bson_size)
    end
  end

  def test_max_bson_size_with_no_reported_max_size
    conn = standard_connection(:connect => false)

    admin_db = Object.new
    admin_db.expects(:command).returns({'ok' => 1, 'ismaster' => 1})
    conn.expects(:[]).with('admin').returns(admin_db)

    conn.connect
    assert_equal Mongo::DEFAULT_MAX_BSON_SIZE, conn.max_bson_size
  end

  def test_connection_activity
    conn = standard_connection
    assert conn.active?

    conn.primary_pool.close
    assert !conn.active?

    # Simulate a dropped connection.
    dropped_socket = mock('dropped_socket')
    dropped_socket.stubs(:read).raises(Errno::ECONNRESET)
    dropped_socket.stubs(:send).raises(Errno::ECONNRESET)
    dropped_socket.stub_everything

    conn.primary_pool.host = 'localhost'
    conn.primary_pool.port = Mongo::Connection::DEFAULT_PORT
    conn.primary_pool.instance_variable_set("@pids", {dropped_socket => Process.pid})
    conn.primary_pool.instance_variable_set("@sockets", [dropped_socket])

    assert !conn.active?
  end

  context "Saved authentications" do
    setup do
      @conn = standard_connection
      @auth = {'db_name' => 'test', 'username' => 'bob', 'password' => 'secret'}
      @conn.add_auth(@auth['db_name'], @auth['username'], @auth['password'])
    end

    teardown do
      @conn.clear_auths
    end

    should "save the authentication" do
      assert_equal @auth, @conn.auths[0]
    end

    should "replace the auth if given a new auth for the same db" do
      auth = {'db_name' => 'test', 'username' => 'mickey', 'password' => 'm0u53'}
      @conn.add_auth(auth['db_name'], auth['username'], auth['password'])
      assert_equal 1, @conn.auths.length
      assert_equal auth, @conn.auths[0]
    end

    should "remove auths by database" do
      @conn.remove_auth('non-existent database')
      assert_equal 1, @conn.auths.length

      @conn.remove_auth('test')
      assert_equal 0, @conn.auths.length
    end

    should "remove all auths" do
      @conn.clear_auths
      assert_equal 0, @conn.auths.length
    end
  end

  context "Socket pools" do
    context "checking out writers" do
      setup do
        @con = standard_connection(:pool_size => 10, :pool_timeout => 10)
        @coll = @con[MONGO_TEST_DB]['test-connection-exceptions']
      end

      should "close the connection on send_message for major exceptions" do
        @con.expects(:checkout_writer).raises(SystemStackError)
        @con.expects(:close)
        begin
          @coll.insert({:foo => "bar"})
        rescue SystemStackError
        end
      end

      should "close the connection on send_message_with_safe_check for major exceptions" do
        @con.expects(:checkout_writer).raises(SystemStackError)
        @con.expects(:close)
        begin
          @coll.insert({:foo => "bar"}, :safe => true)
        rescue SystemStackError
        end
      end

      should "close the connection on receive_message for major exceptions" do
        @con.expects(:checkout_writer).raises(SystemStackError)
        @con.expects(:close)
        begin
          @coll.find.next
        rescue SystemStackError
        end
      end
    end
  end

  context "Connection exceptions" do
    setup do
      @con = standard_connection(:pool_size => 10, :pool_timeout => 10)
      @coll = @con[MONGO_TEST_DB]['test-connection-exceptions']
    end

    should "release connection if an exception is raised on send_message" do
      @con.stubs(:send_message_on_socket).raises(ConnectionFailure)
      assert_equal 0, @con.primary_pool.checked_out.size
      assert_raise ConnectionFailure do
        @coll.insert({:test => "insert"})
      end
      assert_equal 0, @con.primary_pool.checked_out.size
    end

    should "release connection if an exception is raised on send_with_safe_check" do
      @con.stubs(:receive).raises(ConnectionFailure)
      assert_equal 0, @con.primary_pool.checked_out.size
      assert_raise ConnectionFailure do
        @coll.insert({:test => "insert"}, :safe => true)
      end
      assert_equal 0, @con.primary_pool.checked_out.size
    end

    should "release connection if an exception is raised on receive_message" do
      @con.stubs(:receive).raises(ConnectionFailure)
      assert_equal 0, @con.primary_pool.checked_out.size
      assert_raise ConnectionFailure do
        @coll.find.to_a
      end
      assert_equal 0, @con.primary_pool.checked_out.size
    end

    should "show a proper exception message if an IOError is raised while closing a socket" do
      fake_socket = mock('fake_socket')
      fake_socket.stubs(:close).raises(IOError.new)
      fake_socket.stub_everything
      TCPSocket.stubs(:new).returns(fake_socket)

      @con.primary_pool.checkout_new_socket
      @con.primary_pool.expects(:warn)
      assert @con.primary_pool.close
    end
  end
end
