Gem::Specification.new do |s|
  s.name              = 'bson'

  s.version           = File.read(File.join(File.dirname(__FILE__), 'VERSION'))
  s.authors           = ['Tyler Brock', 'Gary Murakami', 'Emily Stolfo', 'Brandon Black', 'Durran Jordan']
  s.email             = 'mongodb-dev@googlegroups.com'
  s.homepage          = 'http://www.mongodb.org'
  s.summary           = 'Ruby implementation of BSON'
  s.description       = 'A Ruby BSON implementation for MongoDB. For more information about Mongo, see http://www.mongodb.org. For more information on BSON, see http://www.bsonspec.org.'
  s.rubyforge_project = 'bson'
  s.license           = 'Apache License Version 2.0'

  if File.exists?('gem-private_key.pem')
    s.signing_key = 'gem-private_key.pem'
    s.cert_chain  = ['gem-public_cert.pem']
  else
    warn 'Warning: No private key present, creating unsigned gem.'
  end

  s.files             = ['bson.gemspec', 'LICENSE', 'VERSION']
  s.files             += ['bin/b2json', 'bin/j2bson', 'lib/bson.rb']
  s.files             += Dir['lib/bson/**/*.rb']

  if RUBY_PLATFORM =~ /java/
    s.platform = 'java'
    s.files    += ['ext/jbson/target/jbson.jar', 'ext/jbson/lib/java-bson.jar']
  else
    s.platform = Gem::Platform::RUBY
  end

  s.executables       = ['b2json', 'j2bson']
  s.require_paths     = ['lib']
  s.has_rdoc          = 'yard'
end
