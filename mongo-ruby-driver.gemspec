# We need to list all of the included files because we aren't allowed to use
# Dir[...] in the github sandbox.
PACKAGE_FILES = ['README.rdoc', 'Rakefile', 'mongo-ruby-driver.gemspec',
                 'bin/bson_benchmark.rb',
                 'bin/mongo_console',
                 'bin/run_test_script',
                 'bin/standard_benchmark',
                 'examples/admin.rb',
                 'examples/benchmarks.rb',
                 'examples/blog.rb',
                 'examples/capped.rb',
                 'examples/cursor.rb',
                 'examples/gridfs.rb',
                 'examples/index_test.rb',
                 'examples/info.rb',
                 'examples/queries.rb',
                 'examples/simple.rb',
                 'examples/strict.rb',
                 'examples/types.rb',
                 'lib/mongo/admin.rb',
                 'lib/mongo/collection.rb',
                 'lib/mongo/connection.rb',
                 'lib/mongo/cursor.rb',
                 'lib/mongo/db.rb',
                 'lib/mongo/gridfs/chunk.rb',
                 'lib/mongo/gridfs/grid_store.rb',
                 'lib/mongo/gridfs.rb',
                 'lib/mongo/errors.rb',
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
                 'lib/mongo/query.rb',
                 'lib/mongo/types/binary.rb',
                 'lib/mongo/types/code.rb',
                 'lib/mongo/types/dbref.rb',
                 'lib/mongo/types/objectid.rb',
                 'lib/mongo/types/regexp_of_holding.rb',
                 'lib/mongo/util/bson.rb',
                 'lib/mongo/util/byte_buffer.rb',
                 'lib/mongo/util/conversions.rb',
                 'lib/mongo/util/ordered_hash.rb',
                 'lib/mongo/util/xml_to_ruby.rb',
                 'lib/mongo.rb']
TEST_FILES = ['test/mongo-qa/_common.rb',
              'test/mongo-qa/admin',
              'test/mongo-qa/capped',
              'test/mongo-qa/count1',
              'test/mongo-qa/dbs',
              'test/mongo-qa/find',
              'test/mongo-qa/find1',
              'test/mongo-qa/gridfs_in',
              'test/mongo-qa/gridfs_out',
              'test/mongo-qa/indices',
              'test/mongo-qa/remove',
              'test/mongo-qa/stress1',
              'test/mongo-qa/test1',
              'test/mongo-qa/update',
              'test/test_admin.rb',
              'test/test_bson.rb',
              'test/test_byte_buffer.rb',
              'test/test_chunk.rb',
              'test/test_collection.rb',
              'test/test_connection.rb',
              'test/test_conversions.rb',
              'test/test_cursor.rb',
              'test/test_db.rb',
              'test/test_db_api.rb',
              'test/test_db_connection.rb',
              'test/test_grid_store.rb',
              'test/test_message.rb',
              'test/test_objectid.rb',
              'test/test_ordered_hash.rb',
              'test/test_threading.rb',
              'test/test_round_trip.rb']

Gem::Specification.new do |s|
  s.name = 'mongo'

  # be sure to change this constant in lib/mongo.rb as well
  s.version = '0.15.1'

  s.platform = Gem::Platform::RUBY
  s.summary = 'Ruby driver for the MongoDB'
  s.description = 'A Ruby driver for MongoDB. For more information about Mongo, see http://www.mongodb.org.'

  s.require_paths = ['lib']
  s.files = PACKAGE_FILES
  s.test_files = TEST_FILES

  s.has_rdoc = true
  s.rdoc_options = ['--main', 'README.rdoc', '--inline-source']
  s.extra_rdoc_files = ['README.rdoc']

  s.authors = ['Jim Menard', 'Mike Dirolf']
  s.email = 'mongodb-dev@googlegroups.com'
  s.homepage = 'http://www.mongodb.org'
end
