$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), "..", "lib"))

require 'mongo'
require 'benchmark'
require_relative 'benchmark_helper'

##
# Perform 'featherweight' benchmarks. This includes
#
# Common Flat BSON
# Common Nested BSON
# All BSON Types
#
##

#bench_helper = BenchmarkHelper.new('perftest','corpus')
bench_helper = BenchmarkHelper.new('foo','bar')
database = bench_helper.database
collection = bench_helper.collection
print "\n\n\n"



##
# Common Flat BSON
#
#
#
#
##
first = Benchmark.bmbm do |bm|
  bm.report('Featherweight::Common Flat BSON') do

  end
end
print "\n\n\n"



##
# Common Nested BSON
#
##
second = Benchmark.bmbm do |bm|
  bm.report('Featherweight::Common Flat BSON') do

  end
end
print "\n\n\n"



##
# All BSON Types
#
##
third = Benchmark.bmbm do |bm|
  bm.report('Featherweight::ALL BSON Types') do

  end
end
print "\n\n\n"
