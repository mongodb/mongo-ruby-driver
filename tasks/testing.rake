# -*- mode: ruby; -*-
require "rspec/core/rake_task"

desc "Run the default test suite (Ruby)"
task :test => 'test:ruby'

namespace :test do
  DEFAULT_TESTS = ['functional', 'unit', 'bson', 'threading']
  ENV['TEST_MODE'] = 'TRUE'

  desc "Run default test suites with the BSON C-extension enabled."
  task :c do
    ENV['C_EXT'] = 'TRUE'
    Rake::Task['compile:cbson'].invoke
    Rake::Task['test:ruby'].invoke
    ENV['C_EXT'] = nil
  end

  RSpec::Core::RakeTask.new(:spec) do |spec|
    spec.pattern = "spec/**/*_spec.rb"
  end

  desc "Runs default test suites"
  task :ruby do
    if RUBY_VERSION >= "1.9.0" && RUBY_ENGINE == 'ruby'
      if ENV['COVERAGE']
        require 'simplecov'
        SimpleCov.start do
          add_group "Mongo", 'lib/mongo'
          add_group "BSON", 'lib/bson'
          add_filter "/test/"
        end
      end
    end

    DEFAULT_TESTS.each { |t| Rake::Task["test:#{t}"].invoke }
    Rake::Task['test:cleanup'].invoke
  end

  %w(sharded_cluster unit threading auxillary bson tools).each do |suite|
    Rake::TestTask.new(suite.to_sym) do |t|
      t.test_files = FileList["test/#{suite}/*_test.rb"]
      t.libs << 'test'
    end
  end

  Rake::TestTask.new(:replica_set) do |t|
    t.test_files = FileList['test/replica_set/*_test.rb'] - [
      'test/replica_set/complex_connect_test.rb',
      'test/replica_set/count_test.rb',
      'test/replica_set/read_preference_test.rb'
    ]
    t.libs << 'test'
  end

  Rake::TestTask.new(:functional) do |t|
    t.test_files = FileList['test/functional/*_test.rb'] - [
      "test/functional/db_api_test.rb",
      "test/functional/pool_test.rb",
      "test/functional/grid_io_test.rb",
      "test/functional/grid_test.rb"
    ]
    t.libs << 'test'
  end

  desc "Runs test cleanup"
  task :cleanup do |t|
    puts "[CLEAN-UP] Dropping test databases..."
    $LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
    require 'mongo'
    client = Mongo::MongoClient.new(
      ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost',
      ENV['MONGO_RUBY_DRIVER_PORT'] || Mongo::MongoClient::DEFAULT_PORT)
    client.database_names.each {|name| client.drop_database(name) if name =~ /^ruby-test/ }

    if File.directory?('data')
      puts "[CLEAN-UP] Removing replica set data files..."
      FileUtils.rm_rf 'data'
    end
  end

end
