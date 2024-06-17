# frozen_string_literal: true
# rubocop:todo all

lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'mongo/version'

Gem::Specification.new do |s|
  s.name              = 'mongo'
  s.version           = Mongo::VERSION
  s.platform          = Gem::Platform::RUBY
  s.authors           = [ 'The MongoDB Ruby Team' ]
  s.email             = 'dbx-ruby@mongodb.com'
  s.homepage          = 'https://mongodb.com/docs/ruby-driver/'
  s.summary           = 'Ruby driver for MongoDB'
  s.license           = 'Apache-2.0'
  s.description       = <<~DESC
    A pure-Ruby driver for connecting to, querying, and manipulating MongoDB
    databases. Officially developed and supported by MongoDB, with love for
    the Ruby community.
  DESC

  s.metadata = {
    'bug_tracker_uri' => 'https://jira.mongodb.org/projects/RUBY',
    'changelog_uri' => 'https://github.com/mongodb/mongo-ruby-driver/releases',
    'homepage_uri' => 'https://mongodb.com/docs/ruby-driver/',
    'documentation_uri' => 'https://mongodb.com/docs/ruby-driver/current/tutorials/quick-start/',
    'source_code_uri' => 'https://github.com/mongodb/mongo-ruby-driver',
  }

  s.files             = Dir.glob('{bin,lib,spec}/**/*')
  s.files             += %w[mongo.gemspec LICENSE README.md CONTRIBUTING.md Rakefile]
  s.test_files        = Dir.glob('spec/**/*')

  s.executables       = ['mongo_console']
  s.require_paths     = ['lib']
  s.bindir            = 'bin'

  s.required_ruby_version = ">= 2.5"

  s.add_dependency 'bson', '>=4.14.1', '<6.0.0'
end
