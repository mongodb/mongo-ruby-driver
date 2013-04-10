Gem::Specification.new do |s|
  s.name              = 'mongo'

  s.version           = File.read(File.join(File.dirname(__FILE__), 'VERSION'))
  s.platform          = Gem::Platform::RUBY
  s.authors           = ['Tyler Brock', 'Gary Murakami', 'Emily Stolfo', 'Brandon Black', 'Durran Jordan']
  s.email             = 'mongodb-dev@googlegroups.com'
  s.homepage          = 'http://www.mongodb.org'
  s.summary           = 'Ruby driver for MongoDB'
  s.description       = 'A Ruby driver for MongoDB. For more information about Mongo, see http://www.mongodb.org.'
  s.rubyforge_project = 'mongo'
  s.license           = 'Apache License Version 2.0'

  if File.exists?('gem-private_key.pem')
    s.signing_key = 'gem-private_key.pem'
    s.cert_chain  = ['gem-public_cert.pem']
  else
    warn 'Warning: No private key present, creating unsigned gem.'
  end

  s.files             = ['mongo.gemspec', 'LICENSE', 'VERSION']
  s.files             += ['README.md', 'Rakefile', 'bin/mongo_console']
  s.files             += ['lib/mongo.rb'] + Dir['lib/mongo/**/*.rb']

  s.test_files        = Dir['test/**/*.rb'] - Dir['test/bson/*']
  s.executables       = ['mongo_console']
  s.require_paths     = ['lib']
  s.has_rdoc          = 'yard'

  s.add_dependency('bson', "~> #{s.version}")
end
