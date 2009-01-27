Gem::Specification.new do |s|
  s.name = 'mongo'
  s.version = '0.3.0'
  s.platform = Gem::Platform::RUBY
  s.summary = 'Simple pure-Ruby driver for the 10gen Mongo DB'
  s.description = 'A pure-Ruby driver for the 10gen Mongo DB. For more information about Mongo, see http://www.mongodb.org.'

  s.require_paths = ['lib']
  
  s.files = ['bin/mongo_console', 'bin/validate',
             'examples/benchmarks.rb',
             'examples/blog.rb',
             'examples/index_test.rb',
             'examples/simple.rb',
             'lib/mongo.rb',
             'lib/mongo/admin.rb',
             'lib/mongo/collection.rb',
             'lib/mongo/cursor.rb',
             'lib/mongo/db.rb',
             'lib/mongo/message/get_more_message.rb',
             'lib/mongo/message/insert_message.rb',
             'lib/mongo/message/kill_cursors_message.rb',
             'lib/mongo/message/message.rb',
             'lib/mongo/message/message_header.rb',
             'lib/mongo/message/msg_message.rb',
             'lib/mongo/message/opcodes.rb',
             'lib/mongo/message/query_message.rb',
             'lib/mongo/message/remove_message.rb',
             'lib/mongo/message/update_message.rb',
             'lib/mongo/message.rb',
             'lib/mongo/mongo.rb',
             'lib/mongo/query.rb',
             'lib/mongo/types/binary.rb',
             'lib/mongo/types/dbref.rb',
             'lib/mongo/types/objectid.rb',
             'lib/mongo/types/regexp_of_holding.rb',
             'lib/mongo/types/undefined.rb',
             'lib/mongo/util/bson.rb',
             'lib/mongo/util/byte_buffer.rb',
             'lib/mongo/util/ordered_hash.rb',
             'lib/mongo/util/xml_to_ruby.rb',
             'README.rdoc', 'Rakefile', 'mongo-ruby-driver.gemspec']
  s.test_files = ['tests/test_admin.rb',
                  'tests/test_bson.rb',
                  'tests/test_byte_buffer.rb',
                  'tests/test_cursor.rb',
                  'tests/test_db.rb',
                  'tests/test_db_api.rb',
                  'tests/test_db_connection.rb',
                  'tests/test_message.rb',
                  'tests/test_mongo.rb',
                  'tests/test_objectid.rb',
                  'tests/test_ordered_hash.rb',
                  'tests/test_round_trip.rb']
  
  s.has_rdoc = true
  s.rdoc_options = ['--main', 'README.rdoc', '--inline-source']
  s.extra_rdoc_files = ['README.rdoc']

  s.author = 'Jim Menard'
  s.email = 'jim@10gen.com'
  s.homepage = 'http://www.mongodb.org'
end
