require 'mongo'
require 'benchmark'

# Mongo::Logger.level = Logger::INFO

auth = { username: 'root-user', password: 'password', db_name: 'test', source: 'admin' }
client = Mongo::MongoClient.new(:auths => Set.new([auth]))

collection = client['ruby-driver']['test']

documents = 50000.times.map do |i|
  { name: 'user', index: i }
end

Benchmark.bm do |bm|

  bm.report('Mongo::Collection#insert') do
    collection.insert(documents)
  end

  bm.report('Mongo::Cursor#each') do
    collection.find.each do |document|
    end
  end

  bm.report('Mongo::Collection#update') do
    collection.update({ name: 'user' }, { '$set' => { name: 'user_modified' }}, { multi: true })
  end

  bm.report('Mongo::Collection#remove') do
    collection.remove({ name: 'user_modified' }, { multi: true })
  end
end
