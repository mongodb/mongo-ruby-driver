require './test/test_helper'
include Mongo

class SafeTest < Test::Unit::TestCase
  context "Safe tests: " do
    setup do
      @con = standard_connection(:safe => {:w => 1})
      @db  = @con[MONGO_TEST_DB]
      @col = @db['test-safe']
      @col.create_index([[:a, 1]], :unique => true)
      @col.remove
    end

    should "propogate safe option on insert" do
      @col.insert({:a => 1})

      assert_raise_error(OperationFailure, "duplicate key") do
        @col.insert({:a => 1})
      end
    end

    should "allow safe override on insert" do
      @col.insert({:a => 1})
      @col.insert({:a => 1}, :safe => false)
    end

    should "propogate safe option on update" do
      @col.insert({:a => 1})
      @col.insert({:a => 2})

      assert_raise_error(OperationFailure, "duplicate key") do
        @col.update({:a => 2}, {:a => 1})
      end
    end

    should "allow safe override on update" do
      @col.insert({:a => 1})
      @col.insert({:a => 2})
      @col.update({:a => 2}, {:a => 1}, :safe => false)
    end
  end
end
