$:.unshift(File.join(File.dirname(__FILE__), 'lib'))
require 'bson/version'

Gem::Specification.new do |s|
  s.name = 'bson'
  s.version = BSON::VERSION
  s.summary = 'Ruby implementation of BSON'
  s.description = 'A Ruby BSON implementation for MongoDB. For more information about Mongo, see http://www.mongodb.org. For more information on BSON, see http://www.bsonspec.org.'

  s.rubyforge_project = 'nowarning'
  s.require_paths = ['lib']

  s.files  = ['LICENSE.txt']
  s.files += ['lib/bson.rb'] + Dir['lib/bson/**/*.rb']
  s.files += ['bin/b2json', 'bin/j2bson']
  if RUBY_PLATFORM =~ /java/
    s.files += ['ext/java/jar/jbson.jar', 'ext/java/jar/mongo-2.6.5.jar']
    s.platform = Gem::Platform::JAVA
  else
    s.platform = Gem::Platform::RUBY
  end
  s.test_files = Dir['test/bson/*.rb', 'test/support/hash_with_indifferent_access.rb']
  s.executables = ['b2json', 'j2bson']

  s.has_rdoc = true
  s.authors = ['Tyler Brock', 'Gary Murakami', 'Emily Stolfo', 'Brandon Black']
  s.email = 'mongodb-dev@googlegroups.com'
  s.homepage = 'http://www.mongodb.org'
end
