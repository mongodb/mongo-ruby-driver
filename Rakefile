# -*- mode: ruby; -*-
if RUBY_VERSION < '1.9.0'
  require 'rubygems'
  require 'rubygems/specification'
end

require 'fileutils'
require 'rake/testtask'
require 'rake'
require 'rake/extensiontask'
require 'rake/javaextensiontask'

begin
  require 'git'
  require 'devkit'
  require 'ci/reporter/rake/test_unit'
  rescue LoadError
end

ENV['TEST_MODE'] = 'TRUE'

Rake::ExtensionTask.new('cbson') do |ext|
  ext.lib_dir = "lib/bson_ext"
end

#Rake::JavaExtensionTask.new('jbson') do |ext| # not yet functional
#  ext.ext_dir = 'ext/src/org/jbson'
#end

desc "Compiles and tests MongoDB Ruby driver w/ C extensions."
task :c do
  Rake::Task['compile:cbson'].invoke
  Rake::Task['test:c'].invoke
end

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
    mongo_jar = File.join(jar_dir, 'mongo-2.6.5.jar')

    src_base   = File.join(java_dir, 'src')

    system("javac -Xlint:deprecation -Xlint:unchecked -classpath #{jruby_jar}:#{mongo_jar} #{File.join(src_base, 'org', 'jbson', '*.java')}")
    system("cd #{src_base} && jar cf #{File.join(jar_dir, 'jbson.jar')} #{File.join('.', 'org', 'jbson', '*.class')}")
  end
end

desc "Test the MongoDB Ruby driver."
task :test do
  puts "\nTo test the driver with the C-extensions:\nrake test:c\n\n"
  puts "To test the pure ruby driver: \nrake test:ruby\n\n"
end

task :path do
    $:.unshift(File.join(File.dirname(__FILE__), 'lib'))
end

namespace :test do

  desc "Test the driver with the C extension enabled."
  task :c => :path do
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
  task :ruby => :path do
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
    t.ruby_opts << '-w'
  end

  desc "Run the replica set test suite"
  Rake::TestTask.new(:rs_no_threads) do |t|
    t.test_files = FileList['test/replica_sets/*_test.rb'] - ["test/replica_sets/refresh_with_threads_test.rb"]
    t.verbose    = true
    t.ruby_opts << '-w'
  end

  Rake::TestTask.new(:unit) do |t|
    t.test_files = FileList['test/unit/*_test.rb']
    t.verbose    = true
    t.ruby_opts << '-w'
  end

  Rake::TestTask.new(:functional) do |t|
    t.test_files = FileList['test/*_test.rb']
    t.verbose    = true
    t.ruby_opts << '-w'
  end

  Rake::TestTask.new(:pooled_threading) do |t|
    t.test_files = FileList['test/threading/*_test.rb']
    t.verbose    = true
    t.ruby_opts << '-w'
  end

  Rake::TestTask.new(:auto_reconnect) do |t|
    t.test_files = FileList['test/auxillary/autoreconnect_test.rb']
    t.verbose    = true
    t.ruby_opts << '-w'
  end

  Rake::TestTask.new(:authentication) do |t|
    t.test_files = FileList['test/auxillary/authentication_test.rb']
    t.verbose    = true
    t.ruby_opts << '-w'
  end

  Rake::TestTask.new(:new_features) do |t|
    t.test_files = FileList['test/auxillary/1.4_features.rb']
    t.verbose    = true
    t.ruby_opts << '-w'
  end

  Rake::TestTask.new(:bson) do |t|
    t.test_files = FileList['test/bson/*_test.rb']
    t.verbose    = true
    t.ruby_opts << '-w'
  end

  task :drop_databases => :path do |t|
    puts "Dropping test databases..."
    require 'mongo'
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
  require './lib/mongo/version.rb'
  out = File.join('ydoc', Mongo::VERSION)
  FileUtils.rm_rf('ydoc')
  system "yardoc lib/**/*.rb lib/mongo/**/*.rb lib/bson/**/*.rb -e ./yard/yard_ext.rb -p yard/templates -o #{out} --title MongoRuby-#{Mongo::VERSION} --files docs/TUTORIAL.md,docs/GridFS.md,docs/FAQ.md,docs/REPLICA_SETS.md,docs/WRITE_CONCERN.md,docs/READ_PREFERENCE.md,docs/HISTORY.md,docs/CREDITS.md,docs/RELEASES.md,docs/CREDITS.md,docs/TAILABLE_CURSORS.md"
end

namespace :jenkins do
  task :ci_reporter do
    begin
      require 'ci/reporter/rake/test_unit'
    rescue LoadError
      warn "Warning: Unable to load ci_reporter gem."
    end
  end

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
    `gem build bson.gemspec`
    `gem install --no-rdoc --no-ri bson-*.gem`

    `gem build mongo.gemspec`
    `gem install --no-rdoc --no-ri mongo-*.gem`

    `rm mongo-*.gem`
    `rm bson-*.gem`
  end
  
  desc "Uninstall the optional c extensions"
  task :uninstall_extensions do
    `gem uninstall bson_ext`
  end

  desc "Install the optional c extensions"
  task :install_extensions do
    `gem build bson_ext.gemspec`
    `gem install --no-rdoc --no-ri bson_ext-*.gem`
    `rm bson_ext-*.gem`
  end
end

namespace :ci do
  namespace :test do
    task :c => :path do
      Rake::Task['gem:install'].invoke
      Rake::Task['gem:install_extensions'].invoke
      Rake::Task['test:c'].invoke
    end
  end
end

# Deployment
VERSION_FILES = %w(lib/bson/version.rb lib/mongo/version.rb ext/cbson/version.h)
GEMSPECS = %w(bson.gemspec bson.java.gemspec bson_ext.gemspec mongo.gemspec)

def gem_list(version)
  files = []
  files << "bson-#{version}.gem"
  files << "bson-#{version}-java.gem"
  files << "bson_ext-#{version}.gem"
  files << "mongo-#{version}.gem"
  return files
end

def check_gem_list_existence(version)
  gem_list(version).each do |filename|
    if !File.exists?(filename)
      raise "#{filename} does not exist!"
    end
  end
end

def check_version(version)
  if !(version =~ /\d\.\d\.\d/)
    raise "Must specify a valid version (e.g., x.y.z)"
  end
end

def current_version
  f = File.open("lib/mongo/version.rb")
  str = f.read
  str =~ /VERSION\s+=\s+([.\d"]+)$/
  return $1
end

def change_version(new_version)
  version = current_version
  puts "Changing version from #{version} to #{new_version}"
  VERSION_FILES.each do |filename|
    f = File.open(filename)
    str = f.read
    f.close
    str.gsub!(version, "\"#{new_version}\"")
    File.open(filename, 'w') do |f|
      f.write(str)
    end
  end
end

namespace :deploy do
  desc "Change version to new release"
  task :change_version, [:version] do |t, args|
    check_version(args[:version]) 
    change_version(args[:version])
  end

  desc "Add version files, commit, tag release"
  task :git_prepare do |t, args|
    g = Git.open(Dir.getwd())
    version = current_version
    to_commit = VERSION_FILES << 'docs/HISTORY.md'
    g.add(to_commit)
    g.commit "RELEASE #{version}"
    g.add_tag("#{version}")
  end

  desc "Push release to github"
  task :git_push do
    g = Git.open(Dir.getwd())
    g.push
  end

  desc "Build all gems"
  task :gem_build do
    `rm *.gem`
    `gem build mongo.gemspec`
    `gem build bson.gemspec`
    `gem build bson.java.gemspec`
    `gem build bson_ext.gemspec`
    puts `ls *.gem`
  end

  desc "Push all gems to RubyGems"
  task :gem_push do |t, args|
    check_gem_list_existence(current_version)
    gem_list.each do |gem|
      puts "Push #{gem} to RubyGems? (y/N)"
      if gets.chomp! == 'y'
        system "gem push #{gem}"
      end
    end
  end
end

task :default => :list

task :list do
  system 'rake -T'
end
