$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), "..", "lib"))

require 'mongo'
require 'benchmark'
require_relative 'benchmark_helper'

##
# Perform 'heavyweight' benchmarks. This includes
#
# LDJSON multi-file import
# LDJSON multi-file export
# GridFS multi-file upload
# GridFS multi-file download
#
##

#bench_helper = BenchmarkHelper.new('perftest','corpus')
bench_helper = BenchmarkHelper.new('foo','bar')
database = bench_helper.database
collection = bench_helper.collection
print "\n\n\n"



##
# LDJSON multi-file import
#
#
#
#
##
first = Benchmark.bmbm do |bm|
  bm.report('Heavyweight::LDJSON multi-file import') do

  end
end
print "\n\n\n"



##
# LDJSON multi-file export
#
##
second = Benchmark.bmbm do |bm|
  bm.report('Heavyweight::LDJSON multi-file export') do

  end
end
print "\n\n\n"



##
# GridFS multi-file upload
#
##
third = Benchmark.bmbm do |bm|
  bm.report('Heavyweight::GridFS multi-file upload') do

  end
end
print "\n\n\n"



##
# GridFS multi-file download
#
##
fourth = Benchmark.bmbm do |bm|
  bm.report('Heavyweight::GridFS multi-file download') do

  end
end
print "\n\n\n"

