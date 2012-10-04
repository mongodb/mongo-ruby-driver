require 'test_helper'

class TestTimeout < Test::Unit::TestCase
  def test_op_timeout
    connection = standard_connection(:op_timeout => 2)
    
    admin = connection.db('admin')

    command = BSON::OrderedHash.new
    command[:sleep] = 1
    command[:secs] = 1
    # Should not timeout
    assert admin.command(command)
   
    # Should timeout
    command[:secs] = 3
    assert_raise Mongo::OperationTimeout do
      admin.command(command) 
    end

  end
=begin
  def test_ssl_op_timeout
    connection = standard_connection(:op_timeout => 1, :ssl => true)
    
    coll = connection.db(MONGO_TEST_DB).collection("test")
    coll.insert({:a => 1})
   
    # Should not timeout
    assert coll.find_one({"$where" => "sleep(100); return true;"})

    # Should timeout 
    assert_raise Mongo::OperationTimeout do
      coll.find_one({"$where" => "sleep(5 * 1000); return true;"})
    end

    coll.remove
  end
=end
end
