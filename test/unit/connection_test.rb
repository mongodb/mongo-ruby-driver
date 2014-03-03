# Copyright (C) 2009-2013 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'test_helper'

class ConnectionUnitTest < Test::Unit::TestCase
  context "Mongo::MongoClient initialization " do
    context "given a single node" do
      setup do
        @connection = Mongo::Connection.new('localhost', 27017, :safe => true, :connect => false)
        TCPSocket.stubs(:new).returns(new_mock_socket)

        admin_db = new_mock_db
        admin_db.expects(:command).returns({'ok' => 1, 'ismaster' => 1})
        @connection.expects(:[]).with('admin').returns(admin_db)
        @connection.connect
      end

      should "set safe mode true" do
        assert_equal true, @connection.safe
      end

      should "set localhost and port to master" do
        assert_equal 'localhost', @connection.primary_pool.host
        assert_equal 27017, @connection.primary_pool.port
      end

      should "set connection pool to 1" do
        assert_equal 1, @connection.primary_pool.size
      end

      should "default slave_ok to false" do
        assert !@connection.slave_ok?
      end

      should "not raise error if no host or port is supplied" do
        assert_nothing_raised do
          Mongo::Connection.new(:safe => true, :connect => false)
        end
        assert_nothing_raised do
          Mongo::Connection.new('localhost', :safe => true, :connect => false)
        end
      end

      should "warn if invalid options are specified" do
        connection = Mongo::Connection.allocate
        opts = {:connect => false}

        Mongo::ReplSetConnection::REPL_SET_OPTS.each do |opt|
          connection.expects(:warn).with("#{opt} is not a valid option for #{connection.class}")
          opts[opt] = true
        end

        args = ['localhost', 27017, opts]
        connection.send(:initialize, *args)
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
        @connection = Mongo::Connection.from_uri("mongodb://localhost", :connect => false)
        assert_equal ['localhost', 27017], @connection.host_port
      end

      should "set auth source" do
        @connection = Mongo::Connection.from_uri("mongodb://user:pass@localhost?authSource=foo", :connect => false)
        assert_equal 'foo', @connection.auths.first[:source]
      end

      should "set auth mechanism" do
        @connection = Mongo::Connection.from_uri("mongodb://user@localhost?authMechanism=MONGODB-X509", :connect => false)
        assert_equal 'MONGODB-X509', @connection.auths.first[:mechanism]

        assert_raise MongoArgumentError do
          Mongo::Connection.from_uri("mongodb://localhost?authMechanism=INVALID", :connect => false)
        end
      end

      should "allow a complex host names" do
        host_name = "foo.bar-12345.org"
        @connection = Mongo::Connection.from_uri("mongodb://#{host_name}", :connect => false)
        assert_equal [host_name, 27017], @connection.host_port
      end

      should "allow db without username and password" do
        host_name = "foo.bar-12345.org"
        @connection = Mongo::Connection.from_uri("mongodb://#{host_name}/foo", :connect => false)
        assert_equal [host_name, 27017], @connection.host_port
      end

      should "set safe options on connection" do
        host_name = "localhost"
        opts = "safe=true&w=2&wtimeoutMS=1000&fsync=true&journal=true"
        @connection = Mongo::Connection.from_uri("mongodb://#{host_name}/foo?#{opts}", :connect => false)
        assert_equal({:w => 2, :wtimeout => 1000, :fsync => true, :j => true}, @connection.write_concern)
      end

      should "set timeout options on connection" do
        host_name = "localhost"
        opts = "connectTimeoutMS=1000&socketTimeoutMS=5000"
        @connection = Mongo::Connection.from_uri("mongodb://#{host_name}/foo?#{opts}", :connect => false)
        assert_equal 1, @connection.connect_timeout
        assert_equal 5, @connection.op_timeout
      end

      should "parse a uri with a hyphen & underscore in the username or password" do
        @connection = Mongo::Connection.from_uri("mongodb://hyphen-user_name:p-s_s@localhost:27017/db", :connect => false)
        assert_equal ['localhost', 27017], @connection.host_port

        auth_hash = {
          :db_name   => 'db',
          :username  => 'hyphen-user_name',
          :password  => 'p-s_s',
          :source    => 'db',
          :mechanism => Authentication::DEFAULT_MECHANISM,
          :extra     => {}
        }
        assert_equal auth_hash, @connection.auths.first
      end

      should "attempt to connect" do
        TCPSocket.stubs(:new).returns(new_mock_socket)
        @connection = Mongo::Connection.from_uri("mongodb://localhost", :connect => false)

        admin_db = new_mock_db
        admin_db.expects(:command).returns({'ok' => 1, 'ismaster' => 1})
        @connection.expects(:[]).with('admin').returns(admin_db)
        @connection.connect
      end

      should "raise an error on invalid uris" do
        assert_raise MongoArgumentError do
          Mongo::Connection.from_uri("mongo://localhost", :connect => false)
        end

        assert_raise MongoArgumentError do
          Mongo::Connection.from_uri("mongodb://localhost:abc", :connect => false)
        end
      end

      should "require password if using legacy auth and username present" do
        assert Mongo::Connection.from_uri("mongodb://kyle:jones@localhost/db", :connect => false)

        assert_raise MongoArgumentError do
          Mongo::Connection.from_uri("mongodb://kyle:@localhost", :connect => false)
        end

        assert_raise MongoArgumentError do
          Mongo::Connection.from_uri("mongodb://kyle@localhost", :connect => false)
        end
      end
    end

    context "initializing with ENV['MONGODB_URI']" do
      should "parse a simple uri" do
        uri = "mongodb://localhost?connect=false"
        with_preserved_env_uri(uri) do
          @connection = Mongo::Connection.new
          assert_equal ['localhost', 27017], @connection.host_port
        end
      end

      should "set auth source" do
        uri = "mongodb://user:pass@localhost?authSource=foo&connect=false"
        with_preserved_env_uri(uri) do
          @connection = Mongo::Connection.new
          assert_equal 'foo', @connection.auths.first[:source]
        end
      end

      should "set auth mechanism" do
        uri = "mongodb://user@localhost?authMechanism=MONGODB-X509&connect=false"
        with_preserved_env_uri(uri) do
          @connection = Mongo::Connection.new
          assert_equal 'MONGODB-X509', @connection.auths.first[:mechanism]

          ENV['MONGODB_URI'] = "mongodb://user@localhost?authMechanism=INVALID&connect=false"
          assert_raise MongoArgumentError do
            Mongo::Connection.new
          end
        end
      end

      should "allow a complex host names" do
        host_name = "foo.bar-12345.org"
        uri = "mongodb://#{host_name}?connect=false"
        with_preserved_env_uri(uri) do
          @connection = Mongo::Connection.new
          assert_equal [host_name, 27017], @connection.host_port
        end
      end

      should "allow db without username and password" do
        host_name = "foo.bar-12345.org"
        uri = "mongodb://#{host_name}/foo?connect=false"
        with_preserved_env_uri(uri) do
          @connection = Mongo::Connection.new
          assert_equal [host_name, 27017], @connection.host_port
        end
      end

      should "set safe options on connection" do
        host_name = "localhost"
        opts = "safe=true&w=2&wtimeoutMS=1000&fsync=true&journal=true&connect=false"
        uri = "mongodb://#{host_name}/foo?#{opts}"
        with_preserved_env_uri(uri) do
          @connection = Mongo::Connection.new
          assert_equal({:w => 2, :wtimeout => 1000, :fsync => true, :j => true}, @connection.safe)
        end
      end

      should "set timeout options on connection" do
        host_name = "localhost"
        opts = "connectTimeoutMS=1000&socketTimeoutMS=5000&connect=false"
        uri = "mongodb://#{host_name}/foo?#{opts}"
        with_preserved_env_uri(uri) do
          @connection = Mongo::Connection.new
          assert_equal 1, @connection.connect_timeout
          assert_equal 5, @connection.op_timeout
        end
      end

      should "parse a uri with a hyphen & underscore in the username or password" do
        uri = "mongodb://hyphen-user_name:p-s_s@localhost:27017/db?connect=false"
        with_preserved_env_uri(uri) do
          @connection = Mongo::Connection.new
          assert_equal ['localhost', 27017], @connection.host_port

          auth_hash = {
            :db_name   => 'db',
            :username  => 'hyphen-user_name',
            :password  => 'p-s_s',
            :source    => 'db',
            :mechanism => Authentication::DEFAULT_MECHANISM,
            :extra     => {}
          }
          assert_equal auth_hash, @connection.auths.first
        end
      end

      should "attempt to connect" do
        TCPSocket.stubs(:new).returns(new_mock_socket)
        uri = "mongodb://localhost?connect=false"
        with_preserved_env_uri(uri) do
          @connection = Mongo::Connection.new

          admin_db = new_mock_db
          admin_db.expects(:command).returns({'ok' => 1, 'ismaster' => 1})
          @connection.expects(:[]).with('admin').returns(admin_db)
          @connection.connect
        end
      end

      should "raise an error on invalid uris" do
        uri = "mongo://localhost"
        with_preserved_env_uri(uri) do
          assert_raise MongoArgumentError do
            Mongo::Connection.new
          end

          ENV['MONGODB_URI'] = "mongodb://localhost:abc"
          assert_raise MongoArgumentError do
            Mongo::Connection.new
          end
        end
      end

      should "require password if using legacy auth and username present" do
        uri = "mongodb://kyle:jones@localhost/db?connect=false"
        with_preserved_env_uri(uri) do
          assert Mongo::Connection.new

          ENV['MONGODB_URI'] = "mongodb://kyle:@localhost?connect=false"
          assert_raise MongoArgumentError do
            Mongo::Connection.new
          end

          ENV['MONGODB_URI'] = "mongodb://kyle@localhost?connect=false"
          assert_raise MongoArgumentError do
            Mongo::Connection.new
          end
        end
      end

      should "require password if using PLAIN auth and username present" do
        uri = "mongodb://kyle:jones@localhost/db?connect=false&authMechanism=PLAIN"
        with_preserved_env_uri(uri) do
          assert Mongo::Connection.new

          ENV['MONGODB_URI'] = "mongodb://kyle:@localhost?connect=false&authMechanism=PLAIN"
          assert_raise MongoArgumentError do
            Mongo::Connection.new
          end

          ENV['MONGODB_URI'] = "mongodb://kyle@localhost?connect=false&authMechanism=PLAIN"
          assert_raise MongoArgumentError do
            Mongo::Connection.new
          end
        end
      end
    end
  end
end
