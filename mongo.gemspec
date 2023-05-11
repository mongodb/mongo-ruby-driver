# frozen_string_literal: true
# rubocop:todo all

lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'mongo/version'

Gem::Specification.new do |s|
  s.name              = 'mongo'
  s.version           = Mongo::VERSION
  s.platform          = Gem::Platform::RUBY
  s.authors           = ["The MongoDB Ruby Team"]
  s.email             = "dbx-ruby@mongodb.com"
  s.homepage          = 'https://mongodb.com/docs/ruby-driver/'
  s.summary           = 'Ruby driver for MongoDB'
  s.description       = 'A Ruby driver for MongoDB'
  s.license           = 'Apache-2.0'

  s.metadata = {
    'bug_tracker_uri' => 'https://jira.mongodb.org/projects/RUBY',
    'changelog_uri' => 'https://github.com/mongodb/mongo-ruby-driver/releases',
    'documentation_uri' => 'https://mongodb.com/docs/ruby-driver/',
    'homepage_uri' => 'https://mongodb.com/docs/ruby-driver/',
    'source_code_uri' => 'https://github.com/mongodb/mongo-ruby-driver',
  }

  if File.exist?('gem-private_key.pem')
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

  s.required_ruby_version = ">= 2.5"

  # For testing driver against bson master we need to depend on bson < 6.0.0
  # but in release version we want to depend on bson < 5.0.0.
  if %w(1 yes true).include?(ENV['MONGO_RUBY_DRIVER_BSON_MASTER'])
    s.add_dependency 'bson', '>=4.13.0', '<6.0.0'
  else
    s.add_dependency 'bson', '>=4.14.1', '<5.0.0'
  end
end
