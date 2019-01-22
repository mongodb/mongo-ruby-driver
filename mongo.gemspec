lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'mongo/version'

Gem::Specification.new do |s|
  s.name              = 'mongo'
  # The dup call makes `bundle install` work on ruby 1.9.3.
  # Without it rubygems tries to modify version which fails because
  # Mongo::VERSION is frozen.
  s.version           = Mongo::VERSION.dup
  s.platform          = Gem::Platform::RUBY

  s.authors           = ['Tyler Brock', 'Emily Stolfo', 'Durran Jordan']
  s.email             = 'mongodb-dev@googlegroups.com'
  s.homepage          = 'http://www.mongodb.org'
  s.summary           = 'Ruby driver for MongoDB'
  s.description       = 'A Ruby driver for MongoDB'
  s.license           = 'Apache-2.0'

  if File.exists?('gem-private_key.pem')
    s.signing_key     = 'gem-private_key.pem'
    s.cert_chain      = ['gem-public_cert.pem']
  else
    warn "[#{s.name}] Warning: No private key present, creating unsigned gem."
  end

  s.files             = Dir.glob('{bin,lib,spec}/**/*')
  s.files             += %w[mongo.gemspec LICENSE README.md CONTRIBUTING.md Rakefile]
  s.test_files        = Dir.glob('spec/**/*')

  s.executables       = ['mongo_console']
  s.require_paths     = ['lib']
  s.bindir            = 'bin'

  s.add_dependency 'bson', '>=4.4.2', '<5.0.0'
end
