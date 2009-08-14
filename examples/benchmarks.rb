require "benchmark"

$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'

host = ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost'
port = ENV['MONGO_RUBY_DRIVER_PORT'] || XGen::Mongo::Driver::Mongo::DEFAULT_PORT

puts "Connecting to #{host}:#{port}"
db = XGen::Mongo::Driver::Mongo.new(host, port).db('ruby-mongo-examples')
coll = db.collection('test')
coll.clear

OBJS_COUNT = 100
TEST_COUNT = 100

puts "Generating benchmark data"
msgs = %w{hola hello aloha ciao}
arr = (0..OBJS_COUNT).collect {|x| { :number => x, :rndm => (rand(5)+1), :msg => msgs[rand(4)] }}

puts "Running benchmark"
Benchmark.bmbm do |results|
  results.report("single object inserts:  ") {
    TEST_COUNT.times {
      coll.clear
      arr.each {|x| coll.insert(x)}
    }
  }
  results.report("multiple object insert: ") {
    TEST_COUNT.times {
      coll.clear
      coll.insert(arr)
    }
  }
  results.report("find_one: ") {
    TEST_COUNT.times {
      coll.find_one(:number => 0)
    }
  }
end

coll.clear
