$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))

require 'mongo'
require 'pp'

include Mongo

host = ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost'
port = ENV['MONGO_RUBY_DRIVER_PORT'] || Connection::DEFAULT_PORT

puts "Connecting to #{host}:#{port}"
db = Connection.new(host, port).db('ruby-mongo-examples')
coll = db.collection('test')

# Remove all records, if any
coll.remove

# Insert three records
coll.insert('a' => 1)
coll.insert('a' => 2)
coll.insert('b' => 3)
coll.insert('c' => 'foo')
coll.insert('c' => 'bar')

# Count.
puts "There are #{coll.count} records."

# Find all records. find() returns a Cursor.
puts "Find all records:"
pp cursor = coll.find.to_a

# Print them. Note that all records have an _id automatically added by the
# database. See pk.rb for an example of how to use a primary key factory to
# generate your own values for _id.
puts "Print each document individually:"
pp cursor.each { |row| pp row }

# See Collection#find. From now on in this file, we won't be printing the
# records we find.
puts "Find one record:"
pp coll.find('a' => 1).to_a

# Find records sort by 'a', skip 1, limit 2 records.
# Sort can be single name, array, or hash.
puts "Skip 1, limit 2, sort by 'a':"
pp coll.find({}, {:skip => 1, :limit => 2, :sort => 'a'}).to_a

# Find all records with 'a' > 1. There is also $lt, $gte, and $lte.
coll.find({'a' => {'$gt' => 1}})
coll.find({'a' => {'$gt' => 1, '$lte' => 3}})

# Find all records with 'a' in a set of values.
puts "Find all records where a is $in [1, 2]:"
pp coll.find('a' => {'$in' => [1,2]}).to_a

puts "Find by regex:"
pp coll.find({'c' => /f/}).to_a

# Print query explanation
puts "Print an explain:"
pp coll.find({'c' => /f/}).explain

# Use a hint with a query. Need an index. Hints can be stored with the
# collection, in which case they will be used with all queries, or they can be
# specified per query, in which case that hint overrides the hint associated
# with the collection if any.
coll.create_index('c')
coll.hint = 'c'

puts "Print an explain with index:"
pp coll.find('c' => /[f|b]/).explain

puts "Print an explain with natural order hint:"
pp coll.find({'c' => /[f|b]/}, :hint => '$natural').explain
