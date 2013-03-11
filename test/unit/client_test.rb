require 'test_helper'
include Mongo

class ClientTest < Test::Unit::TestCase
  context "Mongo::MongoClient initialization " do
    context "given a single node" do
      setup do
        @client = MongoClient.new('localhost', 27017, :connect => false)
        TCPSocket.stubs(:new).returns(new_mock_socket)

        admin_db = new_mock_db
        admin_db.expects(:command).returns({'ok' => 1, 'ismaster' => 1})
        @client.expects(:[]).with('admin').returns(admin_db)
        @client.connect
      end

      should "gle writes by default" do
        assert_equal 1, @client.write_concern[:w]
      end

      should "set localhost and port to master" do
        assert_equal 'localhost', @client.primary_pool.host
        assert_equal 27017, @client.primary_pool.port
      end

      should "set connection pool to 1" do
        assert_equal 1, @client.primary_pool.size
      end

      should "default slave_ok to false" do
        assert !@client.slave_ok?
      end

      should "not raise error if no host or port is supplied" do
        assert_nothing_raised do
          MongoClient.new(:w => 1, :connect => false)
        end
        assert_nothing_raised do
          MongoClient.new('localhost', :w => 1, :connect=> false)
        end
      end

      should "warn if invalid options are specified" do
        client = MongoClient.allocate
        opts = {:connect => false}

        MongoReplicaSetClient::REPL_SET_OPTS.each do |opt|
          client.expects(:warn).with("#{opt} is not a valid option for #{client.class}")
          opts[opt] = true
        end

        args = ['localhost', 27017, opts]
        client.send(:initialize, *args)
      end

      context "given a replica set" do

        should "warn if invalid options are specified" do
          client = MongoReplicaSetClient.allocate
          opts = {:connect => false}

          MongoClient::CLIENT_ONLY_OPTS.each do |opt|
            client.expects(:warn).with("#{opt} is not a valid option for #{client.class}")
            opts[opt] = true
          end

          args = [['localhost:27017'], opts]
          client.send(:initialize, *args)
        end

        should "throw error if superflous arguments are specified" do
          assert_raise MongoArgumentError do
            MongoReplicaSetClient.new(['localhost:27017'], ['localhost:27018'], {:connect => false})
          end
        end
      end
    end

    context "initializing with a unix socket" do
      setup do
          @connection = Mongo::Connection.new('/tmp/mongod.sock', :safe => true, :connect => false)
          UNIXSocket.stubs(:new).returns(new_mock_unix_socket)
      end
      should "parse a unix socket" do
          assert_equal "/tmp/mongod.sock", @connection.host_port.first
      end
    end

    context "initializing with a mongodb uri" do
      should "parse a simple uri" do
        @client = MongoClient.from_uri("mongodb://localhost", :connect => false)
        assert_equal ['localhost', 27017], @client.host_port
      end

      #should "parse a unix socket" do
      #  socket_address = "/tmp/mongodb-27017.sock"
      #  @client = MongoClient.from_uri("mongodb://#{socket_address}")
      #  assert_equal socket_address, @client.host_port.first
      #end

      should "allow a complex host names" do
        host_name = "foo.bar-12345.org"
        @client = MongoClient.from_uri("mongodb://#{host_name}", :connect => false)
        assert_equal [host_name, 27017], @client.host_port
      end

      should "allow db without username and password" do
        host_name = "foo.bar-12345.org"
        @client = MongoClient.from_uri("mongodb://#{host_name}/foo", :connect => false)
        assert_equal [host_name, 27017], @client.host_port
      end

      should "set write concern options on connection" do
        host_name = "localhost"
        opts = "w=2&wtimeoutMS=1000&fsync=true&journal=true"
        @client = MongoClient.from_uri("mongodb://#{host_name}/foo?#{opts}", :connect => false)
        assert_equal({:w => 2, :wtimeout => 1000, :fsync => true, :j => true}, @client.write_concern)
      end

      should "set timeout options on connection" do
        host_name = "localhost"
        opts = "connectTimeoutMS=1000&socketTimeoutMS=5000"
        @client = MongoClient.from_uri("mongodb://#{host_name}/foo?#{opts}", :connect => false)
        assert_equal 1, @client.connect_timeout
        assert_equal 5, @client.op_timeout
      end

      should "parse a uri with a hyphen & underscore in the username or password" do
        @client = MongoClient.from_uri("mongodb://hyphen-user_name:p-s_s@localhost:27017/db", :connect => false)
        assert_equal ['localhost', 27017], @client.host_port
        auth_hash = { :db_name => 'db', :username => 'hyphen-user_name', :password => 'p-s_s' }
        assert_equal auth_hash, @client.auths[0]
      end

      should "attempt to connect" do
        TCPSocket.stubs(:new).returns(new_mock_socket)
        @client = MongoClient.from_uri("mongodb://localhost", :connect => false)

        admin_db = new_mock_db
        admin_db.expects(:command).returns({'ok' => 1, 'ismaster' => 1})
        @client.expects(:[]).with('admin').returns(admin_db)
        @client.connect
      end

      should "raise an error on invalid uris" do
        assert_raise MongoArgumentError do
          MongoClient.from_uri("mongo://localhost", :connect => false)
        end

        assert_raise MongoArgumentError do
          MongoClient.from_uri("mongodb://localhost:abc", :connect => false)
        end
      end

      should "require all of username, if password and db are specified" do
        assert MongoClient.from_uri("mongodb://kyle:jones@localhost/db", :connect => false)

        assert_raise MongoArgumentError do
          MongoClient.from_uri("mongodb://kyle:password@localhost", :connect => false)
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
        @client = MongoClient.new
        assert_equal ['localhost', 27017], @client.host_port
      end

      should "allow a complex host names" do
        host_name = "foo.bar-12345.org"
        ENV['MONGODB_URI'] = "mongodb://#{host_name}?connect=false"
        @client = MongoClient.new
        assert_equal [host_name, 27017], @client.host_port
      end

      should "allow db without username and password" do
        host_name = "foo.bar-12345.org"
        ENV['MONGODB_URI'] = "mongodb://#{host_name}/foo?connect=false"
        @client = MongoClient.new
        assert_equal [host_name, 27017], @client.host_port
      end

      should "set write concern options on connection" do
        host_name = "localhost"
        opts = "w=2&wtimeoutMS=1000&fsync=true&journal=true&connect=false"
        ENV['MONGODB_URI'] = "mongodb://#{host_name}/foo?#{opts}"
        @client = MongoClient.new
        assert_equal({:w => 2, :wtimeout => 1000, :fsync => true, :j => true}, @client.write_concern)
      end

      should "set timeout options on connection" do
        host_name = "localhost"
        opts = "connectTimeoutMS=1000&socketTimeoutMS=5000&connect=false"
        ENV['MONGODB_URI'] = "mongodb://#{host_name}/foo?#{opts}"
        @client = MongoClient.new
        assert_equal 1, @client.connect_timeout
        assert_equal 5, @client.op_timeout
      end

      should "parse a uri with a hyphen & underscore in the username or password" do
        ENV['MONGODB_URI'] = "mongodb://hyphen-user_name:p-s_s@localhost:27017/db?connect=false"
        @client = MongoClient.new
        assert_equal ['localhost', 27017], @client.host_port
        auth_hash = { :db_name => 'db', :username => 'hyphen-user_name', :password => 'p-s_s' }
        assert_equal auth_hash, @client.auths[0]
      end

      should "attempt to connect" do
        TCPSocket.stubs(:new).returns(new_mock_socket)
        ENV['MONGODB_URI'] = "mongodb://localhost?connect=false" # connect=false ??
        @client = MongoClient.new

        admin_db = new_mock_db
        admin_db.expects(:command).returns({'ok' => 1, 'ismaster' => 1})
        @client.expects(:[]).with('admin').returns(admin_db)
        @client.connect
      end

      should "raise an error on invalid uris" do
        ENV['MONGODB_URI'] = "mongo://localhost"
        assert_raise MongoArgumentError do
          MongoClient.new
        end

        ENV['MONGODB_URI'] = "mongodb://localhost:abc"
        assert_raise MongoArgumentError do
          MongoClient.new
        end
      end

      should "require all of username, if password and db are specified" do
        ENV['MONGODB_URI'] = "mongodb://kyle:jones@localhost/db?connect=false"
        assert MongoClient.new

        ENV['MONGODB_URI'] = "mongodb://kyle:password@localhost"
        assert_raise MongoArgumentError do
          MongoClient.new
        end
      end
    end
  end
end
