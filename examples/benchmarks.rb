require "rubygems"
require "benchwarmer"
  
$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'

include XGen::Mongo::Driver

host = ARGV[0] || 'localhost'
port = ARGV[1] || XGen::Mongo::Driver::Mongo::DEFAULT_PORT

puts "Connecting to #{host}:#{port}"
db = Mongo.new(host, port).db('ruby-mongo-examples-complex')
coll = db.collection('test')
coll.clear

OBJS_COUNT = 100
TEST_COUNT = 100

puts "Generating benchmark data"
msgs = %w{hola hello aloha ciao}
arr = OBJS_COUNT.times.map {|x| { :number => x, :rndm => (rand(5)+1), :msg => msgs[rand(4)] }}

puts "Running benchmark"
Benchmark.warmer(TEST_COUNT) do
  report "single object inserts" do
    coll.clear
    arr.each {|x| coll.insert(x)}
  end
  report "multiple object insert" do
    coll.clear
    coll.insert(arr)
  end
end

coll.clear
