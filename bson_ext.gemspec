require File.expand_path('../lib/bson', __FILE__)

VERSION_HEADER = File.open(File.join(File.dirname(__FILE__), 'ext', 'cbson', 'version.h'), "r")
VERSION        = VERSION_HEADER.read.scan(/VERSION "(\d[^"]+)"/)[0][0]
Gem::Specification.new do |s|
  s.name = 'bson_ext'

  s.version  = VERSION
  s.platform = Gem::Platform::RUBY
  s.summary  = 'C extensions for Ruby BSON.'
  s.description = 'C extensions to accelerate the Ruby BSON serialization. For more information about BSON, see http://bsonspec.org.  For information about MongoDB, see http://www.mongodb.org.'

  s.require_paths = ['ext']
  s.files = ['Rakefile', 'bson_ext.gemspec']
  s.files += Dir['ext/**/*.rb'] + Dir['ext/**/*.c'] + Dir['ext/**/*.h']
  s.test_files = []

  s.has_rdoc = false
  s.extensions << 'ext/cbson/extconf.rb'

  s.author = 'Mike Dirolf'
  s.email = 'mongodb-dev@googlegroups.com'
  s.homepage = 'http://www.mongodb.org'
  s.add_dependency('bson', BSON::VERSION)
end
