require File.expand_path("../../test_helper", __FILE__)
include Mongo

class ConnectionTest < Test::Unit::TestCase
  context "Initialization: " do
    context "given a single node" do
      setup do
        @conn = Connection.new('localhost', 27017, :connect => false)
        TCPSocket.stubs(:new).returns(new_mock_socket)

        admin_db = new_mock_db
        admin_db.expects(:command).returns({'ok' => 1, 'ismaster' => 1})
        @conn.expects(:[]).with('admin').returns(admin_db)
        @conn.connect
      end

      should "set localhost and port to master" do
        assert_equal 'localhost', @conn.primary_pool.host
        assert_equal 27017, @conn.primary_pool.port
      end

      should "set connection pool to 1" do
        assert_equal 1, @conn.primary_pool.size
      end

      should "default slave_ok to false" do
        assert !@conn.slave_ok?
      end

      should "warn if invalid options are specified" do
        conn = Connection.allocate
        opts = {:connect => false}

        ReplSetConnection::REPL_SET_OPTS.each do |opt|
          conn.expects(:warn).with("#{opt} is not a valid option for #{conn.class}")
          opts[opt] = true
        end

        args = ['localhost', 27017, opts]
        conn.send(:initialize, *args)
      end

      context "given a replica set" do
        #should "enforce a minimum refresh_interval" do
        #  @conn = ReplSetConnection.new(['localhost:27017'],
        #    :connect => false, :refresh_mode => :sync, :refresh_interval => 10)
        #  assert_equal 60, @conn.refresh_interval
        #end

        should "warn if invalid options are specified" do
          conn = ReplSetConnection.allocate
          opts = {:connect => false}

          Connection::CONNECTION_OPTS.each do |opt|
            conn.expects(:warn).with("#{opt} is not a valid option for #{conn.class}")
            opts[opt] = true
          end

          args = [['localhost:27017'], opts]
          conn.send(:initialize, *args)
        end
      end
    end

    context "initializing with a mongodb uri" do
      should "parse a simple uri" do
        @conn = Connection.from_uri("mongodb://localhost", :connect => false)
        assert_equal ['localhost', 27017], @conn.host_to_try
      end

      should "allow a complex host names" do
        host_name = "foo.bar-12345.org"
        @conn = Connection.from_uri("mongodb://#{host_name}", :connect => false)
        assert_equal [host_name, 27017], @conn.host_to_try
      end

      should "allow db without username and password" do
        host_name = "foo.bar-12345.org"
        @conn = Connection.from_uri("mongodb://#{host_name}/foo", :connect => false)
        assert_equal [host_name, 27017], @conn.host_to_try
      end

      should "set safe options on connection" do
        host_name = "localhost"
        opts = "safe=true&w=2&wtimeoutMS=1000&fsync=true&journal=true"
        @conn = Connection.from_uri("mongodb://#{host_name}/foo?#{opts}", :connect => false)
        assert_equal({:w => 2, :wtimeout => 1000, :fsync => true, :j => true}, @conn.safe)
      end

      should "set timeout options on connection" do
        host_name = "localhost"
        opts = "connectTimeoutMS=1000&socketTimeoutMS=5000"
        @conn = Connection.from_uri("mongodb://#{host_name}/foo?#{opts}", :connect => false)
        assert_equal 1, @conn.connect_timeout
        assert_equal 5, @conn.op_timeout
      end

      should "parse a uri with a hyphen & underscore in the username or password" do
        @conn = Connection.from_uri("mongodb://hyphen-user_name:p-s_s@localhost:27017/db", :connect => false)
        assert_equal ['localhost', 27017], @conn.host_to_try
        auth_hash = { 'db_name' => 'db', 'username' => 'hyphen-user_name', "password" => 'p-s_s' }
        assert_equal auth_hash, @conn.auths[0]
      end

      should "attempt to connect" do
        TCPSocket.stubs(:new).returns(new_mock_socket)
        @conn = Connection.from_uri("mongodb://localhost", :connect => false)

        admin_db = new_mock_db
        admin_db.expects(:command).returns({'ok' => 1, 'ismaster' => 1})
        @conn.expects(:[]).with('admin').returns(admin_db)
        @conn.connect
      end

      should "raise an error on invalid uris" do
        assert_raise MongoArgumentError do
          Connection.from_uri("mongo://localhost", :connect => false)
        end

        assert_raise MongoArgumentError do
          Connection.from_uri("mongodb://localhost:abc", :connect => false)
        end
      end

      should "require all of username, if password and db are specified" do
        assert Connection.from_uri("mongodb://kyle:jones@localhost/db", :connect => false)

        assert_raise MongoArgumentError do
          Connection.from_uri("mongodb://kyle:password@localhost", :connect => false)
        end
      end
    end

    context "initializing with ENV['MONGODB_URI']" do
      setup do
        @old_mongodb_uri = ENV['MONGODB_URI']
      end

      teardown do
        ENV['MONGODB_URI'] = @old_mongodb_uri
      end

      should "parse a simple uri" do
        ENV['MONGODB_URI'] = "mongodb://localhost?connect=false"
        @conn = Connection.new
        assert_equal ['localhost', 27017], @conn.host_to_try
      end

      should "allow a complex host names" do
        host_name = "foo.bar-12345.org"
        ENV['MONGODB_URI'] = "mongodb://#{host_name}?connect=false"
        @conn = Connection.new
        assert_equal [host_name, 27017], @conn.host_to_try
      end

      should "allow db without username and password" do
        host_name = "foo.bar-12345.org"
        ENV['MONGODB_URI'] = "mongodb://#{host_name}/foo?connect=false"
        @conn = Connection.new
        assert_equal [host_name, 27017], @conn.host_to_try
      end

      should "set safe options on connection" do
        host_name = "localhost"
        opts = "safe=true&w=2&wtimeoutMS=1000&fsync=true&journal=true&connect=false"
        ENV['MONGODB_URI'] = "mongodb://#{host_name}/foo?#{opts}"
        @conn = Connection.new
        assert_equal({:w => 2, :wtimeout => 1000, :fsync => true, :j => true}, @conn.safe)
      end

      should "set timeout options on connection" do
        host_name = "localhost"
        opts = "connectTimeoutMS=1000&socketTimeoutMS=5000&connect=false"
        ENV['MONGODB_URI'] = "mongodb://#{host_name}/foo?#{opts}"
        @conn = Connection.new
        assert_equal 1, @conn.connect_timeout
        assert_equal 5, @conn.op_timeout
      end

      should "parse a uri with a hyphen & underscore in the username or password" do
        ENV['MONGODB_URI'] = "mongodb://hyphen-user_name:p-s_s@localhost:27017/db?connect=false"
        @conn = Connection.new
        assert_equal ['localhost', 27017], @conn.host_to_try
        auth_hash = { 'db_name' => 'db', 'username' => 'hyphen-user_name', "password" => 'p-s_s' }
        assert_equal auth_hash, @conn.auths[0]
      end

      should "attempt to connect" do
        TCPSocket.stubs(:new).returns(new_mock_socket)
        ENV['MONGODB_URI'] = "mongodb://localhost?connect=false" # connect=false ??
        @conn = Connection.new

        admin_db = new_mock_db
        admin_db.expects(:command).returns({'ok' => 1, 'ismaster' => 1})
        @conn.expects(:[]).with('admin').returns(admin_db)
        @conn.connect
      end

      should "raise an error on invalid uris" do
        ENV['MONGODB_URI'] = "mongo://localhost"
        assert_raise MongoArgumentError do
          Connection.new
        end

        ENV['MONGODB_URI'] = "mongodb://localhost:abc"
        assert_raise MongoArgumentError do
          Connection.new
        end
      end

      should "require all of username, if password and db are specified" do
        ENV['MONGODB_URI'] = "mongodb://kyle:jones@localhost/db?connect=false"
        assert Connection.new

        ENV['MONGODB_URI'] = "mongodb://kyle:password@localhost"
        assert_raise MongoArgumentError do
          Connection.new
        end
      end
    end
  end
end
