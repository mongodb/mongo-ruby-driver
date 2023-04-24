# frozen_string_literal: true
# rubocop:todo all

require 'ruby-prof'
require 'mongo'

Mongo::Logger.level = Logger::INFO

client = Mongo::Client.new(
  [ '127.0.0.1:27017' ],
  database: 'ruby-driver',
  user: 'root-user',
  password: 'password',
  auth_source: 'admin'
)

collection = client[:test]

documents = 50000.times.map do |i|
  { name: 'user', index: i }
end

inserts = RubyProf.profile do
  collection.insert_many(documents)
end

iteration = RubyProf.profile do
  collection.find.each do |document|
  end
end

updates = RubyProf.profile do
  collection.find(name: 'user').update_many({ '$set' => { name: 'user_modified' }})
end

deletes = RubyProf.profile do
  collection.find(name: 'user_modified').delete_many
end

p 'Inserts:'
RubyProf::FlatPrinter.new(inserts).print(STDOUT, min_percent: 2)
p 'Iteration:'
RubyProf::FlatPrinter.new(iteration).print(STDOUT, min_percent: 2)
p 'Updates:'
RubyProf::FlatPrinter.new(updates).print(STDOUT, min_percent: 2)
p 'Deletes:'
RubyProf::FlatPrinter.new(deletes).print(STDOUT, min_percent: 2)
