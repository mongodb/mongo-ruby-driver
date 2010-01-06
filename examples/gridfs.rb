$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))

require 'mongo'
require 'mongo/gridfs'

include Mongo
include GridFS

host = ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost'
port = ENV['MONGO_RUBY_DRIVER_PORT'] || Connection::DEFAULT_PORT

puts "Connecting to #{host}:#{port}"
db = Connection.new(host, port).db('ruby-mongo-examples')

def dump(db, fname)
  GridStore.open(db, fname, 'r') { |f| puts f.read }
end

# Write a new file
GridStore.open(db, 'foobar', 'w') { |f| f.write("hello, world!") }

# Read it and print out the contents
dump(db, 'foobar')

# Append more data
GridStore.open(db, 'foobar', 'w+') { |f| f.write("\n"); f.puts "line two" }
dump(db, 'foobar')

# Overwrite
GridStore.open(db, 'foobar', 'w') { |f| f.puts "hello, sailor!" }
dump(db, 'foobar')

# File existence tests
puts "File 'foobar' exists: #{GridStore.exist?(db, 'foobar')}"
puts "File 'does-not-exist' exists: #{GridStore.exist?(db, 'does-not-exist')}"

# Read with offset (uses seek)
puts GridStore.read(db, 'foobar', 6, 7)

# Rewind/seek/tell
GridStore.open(db, 'foobar', 'w') { |f|
  f.write "hello, world!"
  f.rewind
  f.write "xyzzz"
  puts f.tell                   # => 5
  f.seek(4)
  f.write('y')
}
dump(db, 'foobar')              # => 'xyzzy'

# Unlink (delete)
GridStore.unlink(db, 'foobar')
puts "File 'foobar' exists after delete: #{GridStore.exist?(db, 'foobar')}"

# Metadata
GridStore.open(db, 'foobar', 'w') { |f| f.write("hello, world!") }
GridStore.open(db, 'foobar', 'r') { |f|
  puts f.content_type
  puts f.upload_date
  puts f.chunk_size
  puts f.metadata.inspect
}

# Add some metadata; change content type
GridStore.open(db, 'foobar', 'w+') { |f|
  f.content_type = 'text/xml'
  f.metadata = {'a' => 1}
}
# Print it
GridStore.open(db, 'foobar', 'r') { |f|
  puts f.content_type
  puts f.upload_date
  puts f.chunk_size
  puts f.metadata.inspect
}

# You can also set metadata when initially writing the file. Setting :root
# means that the file and its chunks are stored in a different root
# collection: instead of gridfs.files and gridfs.chunks, here we use
# my_files.files and my_files.chunks.
GridStore.open(db, 'foobar', 'w',
               :content_type => 'text/plain',
               :metadata => {'a' => 1},
               :chunk_size => 1024 * 4,
               :root => 'my_files') { |f|
  f.puts 'hello, world'
}

