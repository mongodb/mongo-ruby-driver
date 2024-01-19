# frozen_string_literal: true

require_relative '../suite'

desc 'Runs the DriverBench benchmark suite'
task :driver_bench do
  Mongo::DriverBench::Suite.run!
end
