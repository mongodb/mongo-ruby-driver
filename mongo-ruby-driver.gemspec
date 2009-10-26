require "lib/mongo"

Gem::Specification.new do |s|
  s.name = 'mongo'

  s.version = Mongo::VERSION

  s.platform = Gem::Platform::RUBY
  s.summary = 'Ruby driver for the MongoDB'
  s.description = 'A Ruby driver for MongoDB. For more information about Mongo, see http://www.mongodb.org.'

  s.require_paths = ['lib']

  s.files  = ['README.rdoc', 'Rakefile', 'mongo-ruby-driver.gemspec', 'LICENSE.txt']
  s.files += Dir['lib/**/*.rb'] + Dir['examples/**/*.rb'] + Dir['bin/**/*.rb']
  s.test_files = Dir['test/**/*.rb']

  s.has_rdoc = true
  s.test_files = Dir['test/**/*.rb']

  s.has_rdoc = true
  s.rdoc_options = ['--main', 'README.rdoc', '--inline-source']
  s.extra_rdoc_files = ['README.rdoc']

  s.authors = ['Jim Menard', 'Mike Dirolf']
  s.email = 'mongodb-dev@googlegroups.com'
  s.homepage = 'http://www.mongodb.org'
end
