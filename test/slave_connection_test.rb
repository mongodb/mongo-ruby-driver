require 'test/test_helper'

# NOTE: these tests are run only if we can connect to a single MongoDB in slave mode.
class SlaveConnectionTest < Test::Unit::TestCase
  include Mongo

  def self.connect_to_slave
    @@host = ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost'
    @@port = ENV['MONGO_RUBY_DRIVER_PORT'] || Connection::DEFAULT_PORT
    conn = Connection.new(@@host, @@port, :slave_ok => true)
    cmd = conn['admin'].command(:ismaster => 1)
    cmd['ok'] == 1 && cmd['ismaster'] != 1
  end

  if self.connect_to_slave
    puts "Connected to slave; running slave tests."

    def test_connect_to_slave
      assert_raise Mongo::ConfigurationError do 
        @db = Connection.new(@@host, @@port, :slave_ok => false).db('ruby-mongo-demo')
      end
    end

    def test_slave_ok_sent_to_queries
      @db   = Connection.new(@@host, @@port, :slave_ok => true).db('ruby-mongo-demo')
      assert_equal true, @db.slave_ok?
    end
  else
    puts "Not connected to slave; skipping slave connection tests."

    def test_slave_ok_false_on_queries
      @conn = Connection.new(@@host, @@port)
      assert !@conn.slave_ok?
    end
  end
end
