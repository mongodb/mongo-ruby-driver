$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'

include XGen::Mongo::Driver

db = Mongo.new.db('ruby-mongo-demo')
coll = db.collection('test')
coll.clear

doc = {'a' => 1}
coll.insert(doc)

doc = {'a' => 2}
coll.insert(doc)

doc = {'a' => 3}
coll.insert(doc)

puts "There are #{coll.count()} records in the test collection. Here they are:"
coll.find().each { |doc| puts doc.inspect }
