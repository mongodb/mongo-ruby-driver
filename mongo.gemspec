$:.unshift(File.join(File.dirname(__FILE__), 'lib'))
require 'mongo/version'

Gem::Specification.new do |s|
  s.name = 'mongo'

  s.version = File.read(File.join(File.dirname(__FILE__), 'VERSION'))
  s.platform = Gem::Platform::RUBY
  s.summary = 'Ruby driver for MongoDB'
  s.description = 'A Ruby driver for MongoDB. For more information about Mongo, see http://www.mongodb.org.'
  s.rubyforge_project = 'nowarning'

  s.require_paths = ['lib']

  s.files  = ['README.md', 'Rakefile', 'mongo.gemspec', 'LICENSE']
  s.files += ['lib/mongo.rb'] + Dir['lib/mongo/**/*.rb']
  s.files += Dir['docs/**/*.md'] + Dir['examples/**/*.rb'] + Dir['bin/**/*.rb']
  s.files += Dir['bin/mongo_console']
  s.test_files = Dir['test/**/*.rb']

  s.executables = ['mongo_console']

  s.has_rdoc = true
  s.test_files = Dir['test/**/*.rb']

  s.has_rdoc = true
  s.rdoc_options = ['--main', 'README.md', '--inline-source']
  s.extra_rdoc_files = ['README.md']

  s.authors = ['Tyler Brock', 'Gary Murakami', 'Emily Stolfo', 'Brandon Black']
  s.email = 'mongodb-dev@googlegroups.com'
  s.homepage = 'http://www.mongodb.org'

  s.add_dependency('bson', "~> #{Mongo::VERSION}")
end
