# frozen_string_literal: true
# rubocop:todo all

lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'mongo/version'

Gem::Specification.new do |s|
  s.name              = 'mogno'
  s.version           = Mongo::VERSION
  s.platform          = Gem::Platform::RUBY
  s.authors           = ["The MongoDB Ruby Team"]
  s.email             = "dbx-ruby@mongodb.com"
  s.homepage          = 'https://mongodb.com/docs/ruby-driver/'
  s.summary           = 'A dummy instance of the Ruby driver for MongoDB, for testing gem deployment'
  s.description       = 'A dummy instance of the Ruby driver for MongoDB, for testing gem deployment'
  s.license           = 'Apache-2.0'

  s.metadata = {
    'bug_tracker_uri' => 'https://jira.mongodb.org/projects/RUBY',
    'changelog_uri' => 'https://github.com/mongodb/mongo-ruby-driver/releases',
    'documentation_uri' => 'https://mongodb.com/docs/ruby-driver/',
    'homepage_uri' => 'https://mongodb.com/docs/ruby-driver/',
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
