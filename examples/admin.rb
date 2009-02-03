$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'
require 'pp'

include XGen::Mongo::Driver

host = ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost'
port = ENV['MONGO_RUBY_DRIVER_PORT'] || XGen::Mongo::Driver::Mongo::DEFAULT_PORT

puts "Connecting to #{host}:#{port}"
db = Mongo.new(host, port).db('ruby-mongo-examples')
coll = db.collection('test')

# Erase all records from collection, if any
coll.clear

admin = db.admin

# Profiling level set/get
p admin.profiling_level

# Start profiling everything
admin.profiling_level = :all

# Read records, creating a profiling event
coll.find().to_a

# Stop profiling
admin.profiling_level = :off

# Print all profiling info
pp admin.profiling_info

# Destroy the collection
coll.drop
