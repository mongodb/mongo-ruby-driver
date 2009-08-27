$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'

db = Mongo::Connection.new({:left => ["localhost", 27017], :right => ["localhost", 27018]}, nil, :auto_reconnect => true).db("ruby_test")

db['test'].clear
10.times do |i|
  db['test'].save("x" => i)
end

while true do
  begin
    exit() if not db['test'].count() == 10

    x = 0
    db['test'].find().each do |doc|
      x += doc['x']
    end
    exit() if not x == 45
    print "."
    STDOUT.flush
    sleep 1
  rescue
    sleep 1
  end
end
