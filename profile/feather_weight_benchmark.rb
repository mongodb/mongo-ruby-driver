$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), "..", "lib"))

require 'mongo'
require 'benchmark'
require_relative 'benchmark_helper'


# Array of datasets
#FILES = ['flat_bson.json', 'deep_bson.json', 'full_bson.json']
FILES = ['dataset.txt', 'dataset.txt', 'dataset.txt']     #TODO: can't parse the MongoDB extended JSON files, so insignificant dummy data


# Perform 'featherweight' benchmarks. This includes
#
# Common Flat BSON
# Common Nested BSON
# All BSON Types
#
# The featherweight benchmark is intended to measure BSON encoding/decoding tasks,
# to explore BSON codec efficiency.
#
# @param [ Integer ] benchmark_reps Number of repetitions of each benchmark to run.
#
# @return [ Array< Array<Integer> > ] Arrays of benchmark results
#
# @since 2.2.1
def featherweight_benchmark(benchmark_reps)
  results = []
  FILES.each do |dataset_file_name|
    results << encode_decode_bson_helper(dataset_file_name, benchmark_reps)
  end
  results
end



# Runs encode/decode benchmark on a dataset a given number of times
#
# @param [ Integer ] data_file_name Name of dataset file.
# @param [ Integer ] reps Number of repetitions of the benchmark to run.
#
# @return [ [Array<Integer>, Double] ] An array of benchmark wall clock time results and the size of the dataset in MB
#
# @since 2.2.1
def encode_decode_bson_helper(data_file_name, reps)
  data, data_file_size = BenchmarkHelper.load_array_from_file(data_file_name)

  tms_results = Benchmark.bm do |bm|
    reps.times do
      bm.report("Featherweight::#{data_file_name}") do
        data.each do |doc|
          BSON::Document.from_bson(  BSON::Document.new(doc).to_bson  )
        end
      end
    end
  end

  # Get the real time (wall clock time) from the Benchmark::Tms objects
  return tms_results.map { |result| result.real }, data_file_size/1000000.0
end

#featherweight_benchmark(1)