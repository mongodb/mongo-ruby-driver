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
# The featherweight benchmark is intended to test BSON serialization/deserialization
#
##
def featherweight_benchmark!
  #bench_helper = BenchmarkHelper.new('perftest','corpus')
  bench_helper = BenchmarkHelper.new('foo','bar')
  database = bench_helper.database
  collection = bench_helper.collection


  ##
  # Common Flat BSON
  #
  # - Load FLAT_BSON dataset
  #
  # Measure: Encode each document to a BSON byte-string and decode the BSON byte-string back.
  #
  ##
  flat_data = BenchmarkHelper.load_array_from_file('FLAT_BSON.txt')

  first = Benchmark.bm do |bm|
    bm.report('Featherweight::Common Flat BSON') do
      flat_data.each do |doc|
        BSON::Document.from_bson(  BSON::Document.new(doc).to_bson  )
      end
    end
  end


  ##
  # Common Nested BSON
  #
  # - Load DEEP_BSON dataset
  #
  # Measure: Encode each document to a BSON byte-string and decode the BSON byte-string back.
  #
  ##
  deep_data = BenchmarkHelper.load_array_from_file('DEEP_BSON.txt')

  second = Benchmark.bm do |bm|
    bm.report('Featherweight::Common Flat BSON') do
      deep_data.each do |doc|
        BSON::Document.from_bson(  BSON::Document.new(doc).to_bson  )
      end
    end
  end


  ##
  # All BSON Types
  #
  # - Load FULL_BSON dataset
  #
  # Measure: Encode each document to a BSON byte-string and decode the BSON byte-string back.
  #
  ##
  full_data = BenchmarkHelper.load_array_from_file('FULL_BSON.txt')

  third = Benchmark.bm do |bm|
    bm.report('Featherweight::ALL BSON Types') do
      full_data.each do |doc|
        BSON::Document.from_bson(  BSON::Document.new(doc).to_bson  )
      end
    end
  end


  return first, second, third
end