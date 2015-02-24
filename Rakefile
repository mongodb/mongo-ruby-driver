#!/usr/bin/env ruby

require 'rubygems'

begin
  require 'bundler'
  require 'bundler/gem_tasks'
rescue LoadError
  raise '[FAIL] Bundler not found! Install it with ' +
        '`gem install bundler; bundle install`.'
end

default_groups = [:default, :testing]
Bundler.require(*default_groups)

require 'rspec/core/rake_task'

RSpec::Core::RakeTask.new(:spec)
task :default => :spec

namespace :spec do
  if RUBY_VERSION > '1.9' && RUBY_VERSION < '2.2'
    require 'coveralls/rake/task'
    Coveralls::RakeTask.new
    task :ci => [:spec, 'coveralls:push']
  else
    task :ci => [:spec]
  end
end

task :release => :spec do
  system "git tag -a #{Mongo::VERSION} -m 'Tagging release: #{Mongo::VERSION}'"
  system "git push --tags"
  system "gem build mongo.gemspec"
  system "gem push mongo-#{Mongo::VERSION}.gem"
  system "rm mongo-#{Mongo::VERSION}.gem"
end

desc "Generate all documentation"
task :docs => 'docs:yard'

namespace :docs do
  desc "Generate yard documention"
  task :yard do
    out = File.join('docs', Mongo::VERSION)
    FileUtils.rm_rf(out)
    system "yardoc -o #{out} --title mongo-#{Mongo::VERSION}"
  end
end
