require "rubygems"

class Exception
  def errmsg
    "%s: %s\n%s" % [self.class, message, (backtrace || []).join("\n") << "\n"]
  end
end

$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'

include Mongo

host = ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost'
port = ENV['MONGO_RUBY_DRIVER_PORT'] || Connection::DEFAULT_PORT

puts ">> Connecting to #{host}:#{port}"
DB = Connection.new(host, port).db('ruby-mongo-blog')

LINE_SIZE = 120
puts "=" * LINE_SIZE
puts "Adding authors"
authors = DB.collection "authors"
authors.clear
authors.create_index "meta", '_id' => 1, 'name' => 1, 'age' => 1
puts "-" * LINE_SIZE
shaksp = authors << { :name => "William Shakespeare", :email => "william@shakespeare.com", :age => 587 }
puts "shaksp : #{shaksp.inspect}"
borges = authors << { :name => "Jorge Luis Borges", :email => "jorge@borges.com", :age => 123 }
puts "borges : #{borges.inspect}"
puts "-" * LINE_SIZE
puts "authors ordered by age ascending"
puts "-" * LINE_SIZE
authors.find({}, :sort => [{'age' => 1}]).each {|x| puts "%-25.25s : %-25.25s : %3i" % [x['name'], x['email'], x['age']]}

puts "=" * LINE_SIZE
puts "Adding users"
users = DB.collection "users"
users.clear
# users.create_index "meta", :_id => 1, :login => 1, :name => 1
puts "-" * LINE_SIZE
jdoe = users << { :login => "jdoe", :name => "John Doe", :email => "john@doe.com" }
puts "jdoe : #{jdoe.inspect}"
lsmt = users << { :login => "lsmith", :name => "Lucy Smith", :email => "lucy@smith.com" }
puts "lsmt : #{lsmt.inspect}"
puts "-" * LINE_SIZE
puts "users ordered by login ascending"
puts "-" * LINE_SIZE
users.find({}, :sort => [{'login' => 1}]).each {|x| puts "%-10.10s : %-25.25s : %-25.25s" % [x['login'], x['name'], x['email']]}

puts "=" * LINE_SIZE
puts "Adding articles"
articles = DB.collection "articles"
articles.clear
# articles.create_index "meta", :_id => 1, :author_id => 1, :title => 1
puts "-" * LINE_SIZE
begin
  art1 = articles << { :title => "Caminando por Buenos Aires", :body => "Las callecitas de Buenos Aires tienen ese no se que...", :author_id => borges["_id"].to_s }
  puts "art1 : #{art1.inspect}"
rescue => e
  puts "Error: #{e.errmsg}"
end
begin
  art2 = articles << { :title => "I must have seen thy face before", :body => "Thine eyes call me in a new way", :author_id => shaksp["_id"].to_s, :comments => [ { :user_id => jdoe["_id"].to_s, :body => "great article!" } ] }
  puts "art2 : #{art2.inspect}"
rescue => e
  puts "Error: #{e.errmsg}"
end
puts "-" * LINE_SIZE
puts "articles ordered by title ascending"
puts "-" * LINE_SIZE
articles.find({}, :sort => [{'title' => 1}]).each {|x| puts "%-25.25s : %-25.25s" % [x['title'], x['author_id']]}

puts ">> Closing connection"
DB.close
puts "closed"
