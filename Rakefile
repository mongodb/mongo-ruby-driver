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
    desc "Run the common driver micro benchmarking tests"
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

    task :find_one do
      puts "SINGLE_DOC BENCHMARK:: FIND ONE BY ID"
      Mongo::Benchmarking::SingleDoc.run(:find_one)
    end

    task :insert_one_small do
      puts "SINGLE_DOC BENCHMARK:: INSERT ONE SMALL DOCUMENT"
      Mongo::Benchmarking::SingleDoc.run(:insert_one_small)
    end

    task :insert_one_large do
      puts "SINGLE_DOC BENCHMARK:: INSERT ONE LARGE DOCUMENT"
      Mongo::Benchmarking::SingleDoc.run(:insert_one_large)
    end

    task :all => [:command, :find_one, :insert_one_small, :insert_one_large ]
  end

  namespace :multi_doc do
    desc "Run the common driver multi-document benchmarking tests"
    task :find_many do
      puts "MULTI DOCUMENT BENCHMARK:: FIND MANY"
      Mongo::Benchmarking::MultiDoc.run(:find_many)
    end

    task :bulk_insert_small do
      puts "MULTI DOCUMENT BENCHMARK:: BULK INSERT SMALL"
      Mongo::Benchmarking::MultiDoc.run(:bulk_insert_small)
    end

    task :bulk_insert_large do
      puts "MULTI DOCUMENT BENCHMARK:: BULK INSERT LARGE"
      Mongo::Benchmarking::MultiDoc.run(:bulk_insert_large)
    end

    task :gridfs_upload do
      puts "MULTI DOCUMENT BENCHMARK:: GRIDFS UPLOAD"
      Mongo::Benchmarking::MultiDoc.run(:gridfs_upload)
    end

    task :gridfs_download do
      puts "MULTI DOCUMENT BENCHMARK:: GRIDFS DOWNLOAD"
      Mongo::Benchmarking::MultiDoc.run(:gridfs_download)
    end

  end
end
