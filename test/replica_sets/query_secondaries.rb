$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require 'mongo'
require 'test/unit'
require './test/test_helper'

# NOTE: This test expects a replica set of three nodes to be running
# on the local host.
class ReplicaSetQuerySecondariesTest < Test::Unit::TestCase
  include Mongo

  def setup
    @conn = Mongo::Connection.multi([['localhost', 27018]], :read_secondaries => true)
    @db = @conn.db(MONGO_TEST_DB)
    @db.drop_collection("test-sets")
    @coll = @db.collection("test-sets", :safe => {:w => 2, :wtimeout => 100})
  end

  def test_query
    @coll.save({:a => 20})
    @coll.save({:a => 30})
    @coll.save({:a => 40})
    results = []
    @coll.find.each {|r| results << r["a"]}
    assert results.include?(20)
    assert results.include?(30)
    assert results.include?(40)

    puts "Please disconnect the current master."
    gets

    results = []
    rescue_connection_failure do
      @coll.find.each {|r| results << r}
      [20, 30, 40].each do |a|
        assert results.any? {|r| r['a'] == a}, "Could not find record for a => #{a}"
      end
    end
  end

end
