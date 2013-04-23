# -*- mode: ruby; -*-

desc "Run the default test suite (Ruby)"
task :test => ENV.key?('TRAVIS_TEST') ? 'test:default' : 'test:ruby'

# generate distinct SimpleCov command names and pass them via ENV to test_helper
module Rake
  class Task
    @@simplecov_command_name = nil
    alias_method :orig_enhance, :enhance
    def enhance(deps= nil, &block)
      command_name_block = Proc.new do
        old_command_name = @@simplecov_command_name
        @@simplecov_command_name = [@@simplecov_command_name, name].compact.join(' ')
        ENV['SIMPLECOV_COMMAND_NAME'] = @@simplecov_command_name
        block.call
        ENV.delete('SIMPLECOV_COMMAND_NAME')
        @@simplecov_command_name = old_command_name
      end
      orig_enhance(deps, &command_name_block)
    end
  end
end

namespace :test do
  DEFAULT_TESTS = ['functional', 'unit', 'bson', 'threading']
  ENV['TEST_MODE'] = 'TRUE'

  desc "Run default test suites with BSON extensions enabled."
  task :ext do
    ENV.delete('BSON_EXT_DISABLED')
    Rake::Task['compile'].invoke unless RUBY_PLATFORM =~ /java/
    Rake::Task['test:default'].execute
  end

  desc "Runs default test suites in pure Ruby."
  task :ruby do
    ENV['BSON_EXT_DISABLED'] = 'TRUE'
    Rake::Task['test:default'].execute
    ENV.delete('BSON_EXT_DISABLED')
  end

  desc "Runs default test suites"
  task :default do
    DEFAULT_TESTS.each { |t| Rake::Task["test:#{t}"].execute }
    Rake::Task['test:cleanup'].execute
  end

  desc "Runs commit test suites"
  task :commit do
    COMMIT_TESTS = %w(ext ruby replica_set sharded_cluster)
    COMMIT_TESTS.each{|task| puts "test:#{task}"; Rake::Task["test:#{task}"].execute}
    Rake::Task['test:cleanup'].execute
  end

  desc "Runs coverage test suites"
  task :coverage do
    ENV['COVERAGE'] = 'true'
    Rake::Task['test:commit'].invoke
  end

  %w(sharded_cluster unit threading auxillary bson tools).each do |suite|
    Rake::TestTask.new(suite.to_sym) do |t|
      t.test_files = FileList["test/#{suite}/*_test.rb"]
      t.libs << 'test'
    end
  end

  Rake::TestTask.new(:replica_set) do |t|
    disabled = [
      'test/replica_set/complex_connect_test.rb',
      'test/replica_set/count_test.rb',
      'test/replica_set/read_preference_test.rb'
    ]

    t.test_files = FileList['test/replica_set/*_test.rb'] - disabled
    t.libs << 'test'
    #t.verbose = true
    #t.options = '-v'
  end

  Rake::TestTask.new(:functional) do |t|
    t.test_files = FileList['test/functional/*_test.rb'] - [
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
