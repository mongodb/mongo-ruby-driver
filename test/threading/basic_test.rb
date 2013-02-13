require 'test_helper'

class TestThreading < Test::Unit::TestCase

  include Mongo

  def setup
    @client = standard_connection(:pool_size => 10, :pool_timeout => 30)
    @db = @client.db(MONGO_TEST_DB)
    @coll = @db.collection('thread-test-collection')
    @coll.drop

    collections = ['duplicate', 'unique']

    collections.each do |coll_name|
      coll = @db.collection(coll_name)
      coll.drop
      coll.insert("test" => "insert")
      coll.insert("test" => "update")
      instance_variable_set("@#{coll_name}", coll)
    end

    @unique.create_index("test", :unique => true)
  end

  def test_safe_update
    threads = []
    300.times do |i|
      threads << Thread.new do
        if i % 2 == 0
          assert_raise Mongo::OperationFailure do
            @unique.update({"test" => "insert"}, {"$set" => {"test" => "update"}})
          end
        else
          @duplicate.update({"test" => "insert"}, {"$set" => {"test" => "update"}})
          @duplicate.update({"test" => "update"}, {"$set" => {"test" => "insert"}})
        end
      end
    end

    threads.each {|thread| thread.join}
  end

  def test_safe_insert
    threads = []
    300.times do |i|
      threads << Thread.new do
        if i % 2 == 0
          assert_raise Mongo::OperationFailure do
            @unique.insert({"test" => "insert"})
          end
        else
          @duplicate.insert({"test" => "insert"})
        end
      end
    end

    threads.each {|thread| thread.join}
  end

  def test_concurrent_find
    n_threads = 50

    1000.times do |i|
      @coll.insert({ "x" => "a" })
    end

    threads = []
    n_threads.times do |i|
      threads << Thread.new do
        sum = 0
        @coll.find.to_a.size
      end
    end

    thread_values = threads.map(&:value)
    assert thread_values.all?{|v| v == 1000}
    assert_equal thread_values.size, n_threads
  end
end
