Gem::Specification.new do |s|
  s.name              = 'bson_ext'

  s.version           = File.read(File.join(File.dirname(__FILE__), 'VERSION'))
  s.platform          = Gem::Platform::RUBY
  s.authors           = ['Tyler Brock', 'Gary Murakami', 'Emily Stolfo', 'Brandon Black']
  s.email             = 'mongodb-dev@googlegroups.com'
  s.homepage          = 'http://www.mongodb.org'
  s.summary           = 'C extensions for Ruby BSON.'
  s.description       = 'C extensions to accelerate the Ruby BSON serialization. For more information about BSON, see http://bsonspec.org.  For information about MongoDB, see http://www.mongodb.org.'
  s.rubyforge_project = 'bson_ext'

  s.files             = ['Rakefile', 'bson_ext.gemspec']
  s.files             += Dir['ext/**/*.rb'] + Dir['ext/**/*.c'] + Dir['ext/**/*.h']

  s.require_paths     = ['ext/bson_ext']
  s.has_rdoc          = false
  s.extensions        = ['ext/cbson/extconf.rb']

  s.add_dependency('bson', "~> #{s.version}")
end