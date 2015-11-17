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

require_relative "profile/feather_weight_benchmark.rb"
require_relative "profile/light_weight_benchmark.rb"
require_relative "profile/middle_weight_benchmark.rb"
require_relative "profile/heavy_weight_benchmark.rb"

namespace :benchmark do

  task :run_benchmarks do
    puts "Running MMABench benchmarks..."
    p "FEATHERWEIGHT BENCHMARK"
    featherweight_benchmark!
    print "\n\n\n\n"
    p "LIGHTWEIGHT BENCHMARK"
    lightweight_benchmark!
    print "\n\n\n\n"
    p "MIDDLEWEIGHT BENCHMARK"
    middleweight_benchmark!
    print "\n\n\n\n"
    p "HEAVYWEIGHT BENCHMARK"
    heavyweight_benchmark!
  end
end