$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), "..", "lib"))

require 'mongo'
require 'benchmark'
require_relative 'benchmark_helper'

##
# Perform 'middleweight' benchmarks. This includes
#
# Find many and empty the cursor
# Small doc bulk insert
# Large doc bulk insert
# GridFS upload
# GridFS download
#
##
def middleweight_benchmark!

  #bench_helper = BenchmarkHelper.new('perftest','corpus')
  bench_helper = BenchmarkHelper.new('foo','bar')
  database = bench_helper.database
  collection = bench_helper.collection
  print "\n\n\n"
  print "\n\n\n"



  ##
  # Find many and empty the cursor
  #
  #
  #
  #
  ##
  first = Benchmark.bmbm do |bm|
    bm.report('Middleweight::Find many and empty the cursor') do

    end
  end
  print "\n\n\n"



  ##
  # Small doc bulk insert
  #
  ##
  second = Benchmark.bmbm do |bm|
    bm.report('Middleweight::Small doc bulk insert') do

    end
  end
  print "\n\n\n"



  ##
  # Large doc bulk insert
  #
  ##
  third = Benchmark.bmbm do |bm|
    bm.report('Middleweight::Large doc bulk insert') do

    end
  end
  print "\n\n\n"



  ##
  # GridFS upload
  #
  ##
  fourth = Benchmark.bmbm do |bm|
    bm.report('Middleweight::GridFS upload') do

    end
  end
  print "\n\n\n"



  ##
  # GridFS download
  #
  ##
  fifth = Benchmark.bmbm do |bm|
    bm.report('Middleweight::GridFS download') do

    end
  end
  print "\n\n\n"
end
