$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), "..", "lib"))

require 'mongo'
require 'benchmark'
require_relative 'benchmark_helper'


FILES = ['FLAT_BSON.txt', 'DEEP_BSON.txt', 'FULL_BSON.txt']
# TODO: Make the tests into one test that's performed in FILES.each...
# ^though this works less well if each test is put in a method?

##
# Perform 'featherweight' benchmarks. This includes
#
# Common Flat BSON
# Common Nested BSON
# All BSON Types
#
# The featherweight benchmark is intended to measure BSON encoding/decoding tasks,
# to explore BSON codec efficiency
#
##
def featherweight_benchmark!


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
    10.times do
      bm.report('Featherweight::Common Flat BSON') do
        flat_data.each do |doc|
          BSON::Document.from_bson(  BSON::Document.new(doc).to_bson  )
        end
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
    10.times do
      bm.report('Featherweight::Common Nested BSON') do
        deep_data.each do |doc|
          BSON::Document.from_bson(  BSON::Document.new(doc).to_bson  )
        end
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
    10.times do
      bm.report('Featherweight::ALL BSON Types') do
        full_data.each do |doc|
          BSON::Document.from_bson(  BSON::Document.new(doc).to_bson  )
        end
      end
    end
  end


  first_results = first.map {|res| res.real}
  second_results = second.map {|res| res.real}
  third_results = third.map {|res| res.real}
  return first_results, second_results, third_results
end