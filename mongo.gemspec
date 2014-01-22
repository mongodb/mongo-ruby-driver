lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'mongo/version'

Gem::Specification.new do |s|
  s.name              = 'mongo'
  s.rubyforge_project = 'mongo'
  s.version           = Mongo::VERSION
  s.platform          = Gem::Platform::RUBY

  s.authors           = ['Tyler Brock', 'Gary Murakami', 'Emily Stolfo', 'Brandon Black', 'Durran Jordan']
  s.email             = 'mongodb-dev@googlegroups.com'
  s.homepage          = 'http://www.mongodb.org'
  s.summary           = 'Ruby driver for MongoDB'
  s.description       = 'A Ruby driver for MongoDB'
  s.license           = 'Apache License Version 2.0'

  if File.exists?('gem-private_key.pem')
    s.signing_key     = 'gem-private_key.pem'
    s.cert_chain      = ['gem-public_cert.pem']
  else
    warn "[#{s.name}] Warning: No private key present, creating unsigned gem."
  end

  s.files             = Dir.glob('{bin,lib,spec}/**/*')
  s.files             += %w[mongo.gemspec LICENSE README.md CONTRIBUTING.md Rakefile]
  s.test_files        = Dir.glob('spec/**/*')

  s.require_paths     = ['lib']
  s.has_rdoc          = 'yard'
  s.bindir            = 'bin'

  s.add_dependency 'bson', '~> 2.0'
end
