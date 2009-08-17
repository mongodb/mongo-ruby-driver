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
                 'lib/mongo/mongo.rb',
                 'lib/mongo/query.rb',
                 'lib/mongo/types/binary.rb',
                 'lib/mongo/types/code.rb',
                 'lib/mongo/types/dbref.rb',
                 'lib/mongo/types/objectid.rb',
                 'lib/mongo/types/regexp_of_holding.rb',
                 'lib/mongo/types/undefined.rb',
                 'lib/mongo/util/bson.rb',
                 'lib/mongo/util/byte_buffer.rb',
                 'lib/mongo/util/ordered_hash.rb',
                 'lib/mongo/util/xml_to_ruby.rb',
                 'lib/mongo.rb']
TEST_FILES = ['tests/mongo-qa/_common.rb',
              'tests/mongo-qa/admin',
              'tests/mongo-qa/capped',
              'tests/mongo-qa/count1',
              'tests/mongo-qa/dbs',
              'tests/mongo-qa/find',
              'tests/mongo-qa/find1',
              'tests/mongo-qa/gridfs_in',
              'tests/mongo-qa/gridfs_out',
              'tests/mongo-qa/indices',
              'tests/mongo-qa/remove',
              'tests/mongo-qa/stress1',
              'tests/mongo-qa/test1',
              'tests/mongo-qa/update',
              'tests/test_admin.rb',
              'tests/test_bson.rb',
              'tests/test_byte_buffer.rb',
              'tests/test_chunk.rb',
              'tests/test_collection.rb',
              'tests/test_cursor.rb',
              'tests/test_db.rb',
              'tests/test_db_api.rb',
              'tests/test_db_connection.rb',
              'tests/test_grid_store.rb',
              'tests/test_message.rb',
              'tests/test_mongo.rb',
              'tests/test_objectid.rb',
              'tests/test_ordered_hash.rb',
              'tests/test_threading.rb',
              'tests/test_round_trip.rb']

Gem::Specification.new do |s|
  s.name = 'mongo'
  s.version = '0.12'
  s.platform = Gem::Platform::RUBY
  s.summary = 'Ruby driver for the 10gen Mongo DB'
  s.description = 'A Ruby driver for the 10gen Mongo DB. For more information about Mongo, see http://www.mongodb.org.'

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
