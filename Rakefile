# -*- mode: ruby; -*-
require 'rubygems'
require 'rubygems/specification'
require 'fileutils'
require 'rake'
require 'rake/testtask'
require 'rake/gempackagetask'
begin
  require 'rake/contrib/rubyforgepublisher'
rescue LoadError
end
require 'rbconfig'
include Config
ENV['TEST_MODE'] = 'TRUE'

task :java do
  Rake::Task['build:java'].invoke
  Rake::Task['test:ruby'].invoke
end

namespace :build do
  desc "Build the java extensions."
  task :java do
    puts "Building Java extensions..."
    java_dir  = File.join(File.dirname(__FILE__), 'ext', 'java')
    jar_dir   = File.join(java_dir, 'jar')

    jruby_jar = File.join(jar_dir, 'jruby.jar')
    mongo_jar = File.join(jar_dir, 'mongo-2.4.jar')
    bson_jar = File.join(jar_dir, 'bson-2.2.jar')

    src_base   = File.join(java_dir, 'src')

    system("javac -Xlint:unchecked -classpath #{jruby_jar}:#{mongo_jar}:#{bson_jar} #{File.join(src_base, 'org', 'jbson', '*.java')}")
    system("cd #{src_base} && jar cf #{File.join(jar_dir, 'jbson.jar')} #{File.join('.', 'org', 'jbson', '*.class')}")
  end
end

desc "Test the MongoDB Ruby driver."
task :test do
  puts "\nTo test the driver with the C-extensions:\nrake test:c\n\n"
  puts "To test the pure ruby driver: \nrake test:ruby\n\n"
end

namespace :test do

  desc "Test the driver with the C extension enabled."
  task :c do
    ENV['C_EXT'] = 'TRUE'
    if ENV['TEST']
      Rake::Task['test:functional'].invoke
    else
      Rake::Task['test:unit'].invoke
      Rake::Task['test:functional'].invoke
      Rake::Task['test:bson'].invoke
      Rake::Task['test:pooled_threading'].invoke
      Rake::Task['test:drop_databases'].invoke
    end
    ENV['C_EXT'] = nil
  end

  desc "Test the driver using pure ruby (no C extension)"
  task :ruby do
    ENV['C_EXT'] = nil
    if ENV['TEST']
      Rake::Task['test:functional'].invoke
    else
      Rake::Task['test:unit'].invoke
      Rake::Task['test:functional'].invoke
      Rake::Task['test:bson'].invoke
      Rake::Task['test:pooled_threading'].invoke
      Rake::Task['test:drop_databases'].invoke
    end
  end

  desc "Run the replica set test suite"
  Rake::TestTask.new(:rs) do |t|
    t.test_files = FileList['test/replica_sets/*_test.rb']
    t.verbose    = true
  end

  Rake::TestTask.new(:unit) do |t|
    t.test_files = FileList['test/unit/*_test.rb']
    t.verbose    = true
  end

  Rake::TestTask.new(:functional) do |t|
    t.test_files = FileList['test/*_test.rb']
    t.verbose    = true
  end

  Rake::TestTask.new(:pooled_threading) do |t|
    t.test_files = FileList['test/threading/*_test.rb']
    t.verbose    = true
  end

  Rake::TestTask.new(:auto_reconnect) do |t|
    t.test_files = FileList['test/auxillary/autoreconnect_test.rb']
    t.verbose    = true
  end

  Rake::TestTask.new(:authentication) do |t|
    t.test_files = FileList['test/auxillary/authentication_test.rb']
    t.verbose    = true
  end

  Rake::TestTask.new(:new_features) do |t|
    t.test_files = FileList['test/auxillary/1.4_features.rb']
    t.verbose    = true
  end

  Rake::TestTask.new(:bson) do |t|
    t.test_files = FileList['test/bson/*_test.rb']
    t.verbose    = true
  end

  task :drop_databases do |t|
    puts "Dropping test databases..."
    require './lib/mongo'
    con = Mongo::Connection.new(ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost',
      ENV['MONGO_RUBY_DRIVER_PORT'] || Mongo::Connection::DEFAULT_PORT)
    con.database_names.each do |name|
      con.drop_database(name) if name =~ /^ruby-test/
    end
  end
end

desc "Generate RDOC documentation"
task :rdoc do
  version = eval(File.read("mongo.gemspec")).version
  out = File.join('html', version.to_s)
  FileUtils.rm_rf('html')
  system "rdoc --main README.md --op #{out} --inline-source --quiet README.md `find lib -name '*.rb'`"
end

desc "Generate YARD documentation"
task :ydoc do
  require File.join(File.dirname(__FILE__), 'lib', 'mongo')
  out = File.join('ydoc', Mongo::VERSION)
  FileUtils.rm_rf('ydoc')
  system "yardoc lib/**/*.rb lib/mongo/**/*.rb lib/bson/**/*.rb -e yard/yard_ext.rb -p yard/templates -o #{out} --title MongoRuby-#{Mongo::VERSION} --files docs/TUTORIAL.md,docs/GridFS.md,docs/FAQ.md,docs/REPLICA_SETS.md,docs/WRITE_CONCERN.md,docs/HISTORY.md,docs/CREDITS.md,docs/1.0_UPGRADE.md"
end

namespace :bamboo do
  namespace :test do
    task :ruby do
      Rake::Task['test:ruby'].invoke
    end

    task :c do
      Rake::Task['gem:install_extensions'].invoke
      Rake::Task['test:c'].invoke
    end
  end
end

namespace :gem do

  desc "Install the gem locally"
  task :install do
    sh "gem build bson.gemspec"
    sh "gem install --no-rdoc --no-ri bson-*.gem"

    sh "gem build mongo.gemspec"
    sh "gem install --no-rdoc --no-ri mongo-*.gem"

    sh "rm mongo-*.gem"
    sh "rm bson-*.gem"
  end

  desc "Install the optional c extensions"
  task :install_extensions do
    sh "gem build bson_ext.gemspec"
    sh "gem install --no-rdoc --no-ri bson_ext-*.gem"
    sh "rm bson_ext-*.gem"
  end

  desc "Build all gems"
  task :build_all do
    sh "gem build mongo.gemspec"
    sh "gem build bson.gemspec"
    sh "gem build bson.java.gemspec"
    sh "gem build bson_ext.gemspec"
  end

end

namespace :ci do
  namespace :test do
    task :c do
      Rake::Task['gem:install'].invoke
      Rake::Task['gem:install_extensions'].invoke
      Rake::Task['test:c'].invoke
    end
  end
end

task :default => :list

task :list do
  system 'rake -T'
end
