# Copyright (C) 2009-2013 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

default_tasks = ['test:ruby', 'test:ext', 'test:default']
if RUBY_VERSION > '1.9'
  require 'coveralls/rake/task'
  Coveralls::RakeTask.new
  default_tasks << 'coveralls:push'
end

desc "Run the default test suites."
task :test => default_tasks
task :default => :test

# Generates commands for SimpleCov reporting
module Rake
  class Task
    @@simplecov_cmd = nil
    alias_method :orig_enhance, :enhance
    def enhance(deps= nil, &block)
      command_block = Proc.new do
        old_cmd = @@simplecov_cmd
        @@simplecov_cmd = [@@simplecov_cmd, name].compact.join(' ')
        ENV['SCOV_COMMAND'] = @@simplecov_cmd
        block.call
        ENV.delete('SCOV_COMMAND')
        @@simplecov_cmd = old_cmd
      end
      orig_enhance(deps, &command_block)
    end
  end
end

namespace :test do
  ENV['TEST_MODE'] = 'TRUE'

  desc "Run BSON test suite with extensions enabled."
  task :ext do
    ENV.delete('BSON_EXT_DISABLED')
    Rake::Task['compile'].invoke unless RUBY_PLATFORM =~ /java/
    Rake::Task['test:bson'].execute
  end

  desc "Runs BSON test suite in pure Ruby."
  task :ruby do
    ENV['BSON_EXT_DISABLED'] = 'TRUE'
    Rake::Task['test:bson'].execute
    ENV.delete('BSON_EXT_DISABLED')
  end

  desc "Runs default driver test suites."
  task :default do
    %w(unit functional threading).each do |suite|
      Rake::Task["test:#{suite}"].execute
    end
    Rake::Task['test:cleanup'].execute
  end

  desc "Runs commit test suites."
  task :commit do
    %w(ext without_ext default replica_set sharded_cluster).each do |suite|
      puts "[RUNNING] test:#{suite}"
      Rake::Task["test:#{suite}"].execute
    end
    Rake::Task['test:cleanup'].execute
  end

  %w(sharded_cluster unit threading bson tools).each do |suite|
    Rake::TestTask.new(suite.to_sym) do |t|
      t.test_files = FileList["test/#{suite}/*_test.rb"]
      t.libs << 'test'
    end
  end

  Rake::TestTask.new(:functional) do |t|
    t.test_files = FileList['test/functional/*_test.rb'] - [
      'test/functional/grid_io_test.rb',
      'test/functional/grid_test.rb',
      'test/functional/ssl_test.rb'
    ]
    t.libs << 'test'
  end

  Rake::TestTask.new(:replica_set) do |t|
    disabled = [
      'test/replica_set/complex_connect_test.rb',
      'test/replica_set/count_test.rb',
      'test/replica_set/read_preference_test.rb',
      'test/replica_set/ssl_test.rb'
    ]

    t.test_files = FileList['test/replica_set/*_test.rb'] - disabled
    t.libs << 'test'
  end

  desc "Cleans up from all tests."
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
