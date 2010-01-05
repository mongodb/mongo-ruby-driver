require "benchmark"

$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'

host = ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost'
port = ENV['MONGO_RUBY_DRIVER_PORT'] || Mongo::Connection::DEFAULT_PORT

puts "Connecting to #{host}:#{port}"
db = Mongo::Connection.new(host, port).db('ruby-mongo-examples')
coll = db.collection('test')
coll.remove

OBJS_COUNT = 100
TEST_COUNT = 100

puts "Generating benchmark data"
msgs = %w{hola hello aloha ciao}
arr = (0..OBJS_COUNT).collect {|x| { :number => x, :rndm => (rand(5)+1), :msg => msgs[rand(4)] }}

puts "Running benchmark"
Benchmark.bmbm do |results|
  results.report("single object inserts:  ") {
    TEST_COUNT.times {
      coll.remove
      arr.each {|x| coll.insert(x)}
    }
  }
  results.report("multiple object insert: ") {
    TEST_COUNT.times {
      coll.remove
      coll.insert(arr)
    }
  }
  results.report("find_one: ") {
    TEST_COUNT.times {
      coll.find_one(:number => 0)
    }
  }
end

coll.remove
