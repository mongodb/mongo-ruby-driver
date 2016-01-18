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
  task :ci => [:spec]
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

require_relative "profile/benchmarking"

namespace :benchmark do
  desc "Run the driver benchmark tests"

  namespace :micro do
    desc "Run the driver micro benchmark tests"
    task :flat do
      puts "MICRO BENCHMARK:: FLAT"
      Mongo::Benchmarking::Micro.run(:flat)
    end

    task :deep do
      puts "MICRO BENCHMARK:: DEEP"
      Mongo::Benchmarking::Micro.run(:deep)
    end

    task :full do
      puts "MICRO BENCHMARK:: FULL"
      Mongo::Benchmarking::Micro.run(:full)
    end

    task :all => [:flat, :deep, :full ]
  end

  namespace :single_doc do
    desc "Run the common driver single-document benchmarking tests"
    task :command do
      puts "SINGLE-DOC BENCHMARK:: COMMAND"
      Mongo::Benchmarking::SingleDoc.run(:command)
    end

    task :find_one_by_id do
      puts "SINGLE_DOC BENCHMARK:: FIND ONE BY ID"
      Mongo::Benchmarking::SingleDoc.run(:find_one_by_id)
    end

  end
end
