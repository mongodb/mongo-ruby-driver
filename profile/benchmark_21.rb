require 'mongo'
require 'benchmark'

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
  { name: 'user', index: i, embedded: [{ n: i }] }
end

Benchmark.bm do |bm|

  bm.report('Mongo::Collection#insert_many') do
    collection.insert_many(documents)
  end

  bm.report('Mongo::Cursor#each') do
    collection.find.each do |document|
    end
  end

  bm.report('Mongo::Collection::View#update_many') do
    collection.find(name: 'user').update_many({ '$set' => { name: 'user_modified' }})
  end

  bm.report('Mongo::Collection::View#delete_many') do
    collection.find(name: 'user_modified').delete_many
  end
end
