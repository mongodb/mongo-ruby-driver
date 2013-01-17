require 'test_helper'
require 'thread'

class PoolTest < Test::Unit::TestCase
  include Mongo

  def setup
    @client    ||= standard_connection({:pool_size => 500, :pool_timeout => 5})
    @db         = @client.db(MONGO_TEST_DB)
    @collection = @db.collection("pool_test")
  end

  def test_pool_affinity
    pool = Pool.new(@client, TEST_HOST, TEST_PORT, :size => 5)

    threads = []
    10.times do
      threads << Thread.new do
        original_socket = pool.checkout
        pool.checkin(original_socket)
        5000.times do
          socket = pool.checkout
          assert_equal original_socket, socket
          pool.checkin(socket)
        end
      end
    end

    threads.each { |t| t.join }
  end

  def test_pool_affinity_max_size
    8000.times {|x| @collection.insert({:value => x})}
    threads = []
    threads << Thread.new do
      @collection.find({"value" => {"$lt" => 100}}).each {|e| e}
      Thread.pass
      sleep(5)
      @collection.find({"value" => {"$gt" => 100}}).each {|e| e}
    end
    sleep(1)
    threads << Thread.new do
      @collection.find({'$where' => "function() {for(i=0;i<8000;i++) {this.value};}"}).each {|e| e}
    end
    threads.each(&:join)
  end
end
