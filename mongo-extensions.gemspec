# We need to list all of the included files because we aren't allowed to use
# Dir[...] in the github sandbox.
PACKAGE_FILES = ['Rakefile', 'mongo-extensions.gemspec',
                 'ext/cbson/cbson.c',
                 'ext/cbson/extconf.rb']
TEST_FILES = []

Gem::Specification.new do |s|
  s.name = 'mongo_ext'
  s.version = '0.3'
  s.platform = Gem::Platform::RUBY
  s.summary = 'C extensions for the MongoDB Ruby driver'
  s.description = 'C extensions to accelerate the MondoDB Ruby driver. For more information about Mongo, see http://www.mongodb.org.'

  s.require_paths = ['ext']

  s.files = PACKAGE_FILES
  s.test_files = TEST_FILES

  s.has_rdoc = false
  s.extensions << 'ext/cbson/extconf.rb'

  s.author = 'Mike Dirolf'
  s.email = 'mongodb-dev@googlegroups.com'
  s.homepage = 'http://www.mongodb.org'
end
