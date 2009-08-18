$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'
require 'test/unit'

class TestThreading < Test::Unit::TestCase

  include XGen::Mongo::Driver

  @@host = ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost'
  @@port = ENV['MONGO_RUBY_DRIVER_PORT'] || Mongo::DEFAULT_PORT
  @@db = Mongo.new(@@host, @@port).db('ruby-mongo-test')
  @@coll = @@db.collection('thread-test-collection')

  def test_threading
    @@coll.clear

    1000.times do |i|
      @@coll.insert("x" => i)
    end

    threads = []

    10.times do |i|
      threads[i] = Thread.new{
        sum = 0
        @@coll.find().each { |document|
          sum += document["x"]
        }
        assert_equal 499500, sum
      }
    end

    10.times do |i|
      threads[i].join
    end
  end
end
