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

TEST_SUITES = {
  :bson => { :pattern => 'test/bson/*_test.rb' },
  :unit => { :pattern => 'test/unit/**/*_test.rb' },
  :functional => {
    :pattern => 'test/functional/**/*_test.rb',
    :exclude => ['test/functional/grid_io_test.rb',
                 'test/functional/grid_test.rb',
                 'test/functional/ssl_test.rb']
  },
  :threading => { :pattern => 'test/threading/**/*_test.rb' },
  :replica_set => {
    :pattern => 'test/replica_set/**/*_test.rb',
    :exclude => ['test/replica_set/complex_connect_test.rb',
                 'test/replica_set/count_test.rb',
                 'test/replica_set/read_preference_test.rb',
                 'test/replica_set/ssl_test.rb']
  },
  :sharded_cluster => { :pattern => 'test/sharded_cluster/**/*_test.rb' },
  :tools => {
    :pattern => 'test/tools/**/*_test.rb',
    :exclude => ['test/tools/mongo_config_test.rb']
  }
}

if RUBY_VERSION > '1.9'
  require 'coveralls/rake/task'
  Coveralls::RakeTask.new
end

task :test => 'test:ext'
task :default => ['test:without_ext', 'test:ext']

namespace :test do

  ENV['TEST_MODE'] = 'true'

  desc 'Runs all test suites (excludes RS and SC tests under CI)'
  Rake::TestTask.new(:default) do |t|
    enabled_tests = [:bson, :unit, :functional, :threading]
    unless ENV.key?('TRAVIS_CI') || ENV.key?('JENKINS_CI')
      enabled_tests += [:replica_set, :sharded_cluster]
    end

    files = []
    enabled_tests.each do |suite|
      config = TEST_SUITES[suite]
      files << FileList[config[:pattern]]
      files.flatten!
      files = files - config[:exclude] if config[:exclude]
    end

    t.test_files = files
    t.libs << 'test'
  end
  task :commit => 'default'

  desc 'Outputs diagnostic information for troubleshooting test failures.'
  task :diagnostic do
    puts <<-MSG
    [Diagnostic Info]
    Ruby Version:    #{RUBY_VERSION}
    Ruby Platform:   #{RUBY_PLATFORM}
    Source HEAD:     #{`git rev-parse HEAD | tr -d '\n'`}
    Source Branch:   #{`git rev-parse --abbrev-ref HEAD | tr -d '\n'`}
    MongoDB Version: #{`mongod --version | egrep -o 'v[0-9]+\.[0-9]+\.[0-9]+([-_\.][a-zA-Z0-9]+)?' | tr -d '\n'`}
    MSG
  end

  desc 'Runs all test suites with extensions.'
  task :ext => 'test:cleanup' do
    puts '[INFO] Enabling BSON extension...'
    ENV.delete('BSON_EXT_DISABLED')
    Rake::Task['compile'].invoke unless RUBY_PLATFORM =~ /java/
    Rake::Task['test:default'].reenable
    Rake::Task['test:default'].invoke
  end

  desc 'Runs all test suites without any extensions.'
  task :without_ext => 'test:cleanup'  do
    puts '[INFO] Disabling BSON extension...'
    ENV['BSON_EXT_DISABLED'] = 'true'
    Rake::Task['test:default'].reenable
    Rake::Task['test:default'].invoke
    ENV.delete('BSON_EXT_DISABLED')
  end
  task :ruby => 'test:without_ext'

  # Generated tasks for individual test suites
  TEST_SUITES.each do |suite, config|
    files = FileList[config[:pattern]]
    files = files - config[:exclude] if config[:exclude]
    Rake::TestTask.new(suite) do |t|
      t.test_files = files
      t.libs << 'test'
    end
  end

  # Runs after all tests and automatically as a pre-requisite in some cases.
  # There's no need to ever invoke this directly.
  task :cleanup do |t|
    begin
      $LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
      require 'mongo'
      client = Mongo::MongoClient.new(ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost',
                                      ENV['MONGO_RUBY_DRIVER_PORT'] || Mongo::MongoClient::DEFAULT_PORT)

      if client.server_version >= '2.7.1'
        admin = client['admin']
        admin.add_user('admin', 'password', nil, :roles => [ 'dbAdminAnyDatabase',
                                                             'userAdminAnyDatabase',
                                                             'readWriteAnyDatabase' ])
        admin.authenticate('admin', 'password')
      end

      client.database_names.each do |db_name|
        if db_name =~ /^ruby_test*/
          puts "[CLEAN-UP] Dropping '#{db_name}'..."
          client.drop_database(db_name)
        end
      end

      if client.server_version >= '2.7.1'
        admin.command({ :dropAllUsersFromDatabase => 1 })
        admin.logout
      end
    rescue Mongo::ConnectionFailure => e
      # moving on anyway
    end

    %w(data tmp coverage lib/bson_ext).each do |dir|
      if File.directory?(dir)
        puts "[CLEAN-UP] Removing '#{dir}'..."
        FileUtils.rm_rf(dir)
      end
    end

    t.reenable
  end

end

Rake.application.top_level_tasks << 'test:cleanup'
