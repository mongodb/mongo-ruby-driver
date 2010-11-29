# This code assumes a running replica set with at least one node at localhost:27017.
require 'mongo'

cons = []

10.times do
  cons << Mongo::Connection.multi([['localhost', 27017]], :read_secondary => true)
end

ports = cons.map do |con|
  con.read_pool.port
end

puts "These ten connections will read from the following ports:"
p ports

cons[rand(10)]['foo']['bar'].remove
100.times do |n|
  cons[rand(10)]['foo']['bar'].insert({:a => n})
end

100.times do |n|
  p cons[rand(10)]['foo']['bar'].find_one({:a => n})
end
