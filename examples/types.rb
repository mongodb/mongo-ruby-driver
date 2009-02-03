$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'
require 'pp'

include XGen::Mongo::Driver

host = ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost'
port = ENV['MONGO_RUBY_DRIVER_PORT'] || XGen::Mongo::Driver::Mongo::DEFAULT_PORT

puts "Connecting to #{host}:#{port}"
db = Mongo.new(host, port).db('ruby-mongo-examples')
coll = db.collection('test')

# Remove all records, if any
coll.clear

# Insert record with all sorts of values
coll.insert('array' => [1, 2, 3],
            'string' => 'hello',
            'hash' => {'a' => 1, 'b' => 2},
            'date' => Time.now, # milliseconds only; microseconds are not stored
            'oid' => ObjectID.new,
            'binary' => Binary.new([1, 2, 3]),
            'int' => 42,
            'float' => 33.33333,
            'regex' => /foobar/i,
            'boolean' => true,
            '$where' => 'this.x == 3', # special case of string
            'dbref' => DBRef.new(nil, 'dbref', db, coll.name, ObjectID.new),

# NOTE: the undefined type is not saved to the database properly. This is a
# Mongo bug. However, the undefined type may go away completely.
#             'undef' => Undefined.new,

            'null' => nil,
            'symbol' => :zildjian)

pp coll.find().next_object

coll.clear
