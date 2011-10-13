$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require './test/replica_sets/rs_test_helper'
require 'logger'

# TODO: enable this once we enable reads from tags.
class ReadPreferenceTest < Test::Unit::TestCase
  include Mongo

  def test_long_write_with_async_refresh
    log = Logger.new("test.log")
    conn = ReplSetConnection.new([RS.host, RS.ports[0], RS.host, RS.ports[1]],
          :read => :secondary, :pool_size => 50,
          :refresh_mode => :sync, :refresh_interval => 5, :logger => log)

    db = conn.db(MONGO_TEST_DB)
    db.drop_collection("test-sets")
    col = db['mongo-test']

    100000.times do |n|
      col.insert({:n => n, :str => "0000000000"})
    end

    assert col.find.to_a
    col.remove
  end

  # TODO: enable this once we enable reads from tags.
  # def test_query_tagged
  #   col = @db['mongo-test']

  #   col.insert({:a => 1}, :safe => {:w => 3})
  #   col.find_one({}, :read => {:db => "main"})
  #   col.find_one({}, :read => {:dc => "ny"})
  #   col.find_one({}, :read => {:dc => "sf"})

  #   assert_raise Mongo::NodeWithTagsNotFound do
  #     col.find_one({}, :read => {:foo => "bar"})
  #   end

  #   threads = []
  #   100.times do
  #     threads << Thread.new do
  #       col.find_one({}, :read => {:dc => "sf"})
  #     end
  #   end

  #   threads.each {|t| t.join }

  #   col.remove
  # end

  #def teardown
  #  RS.restart_killed_nodes
  #end

end
