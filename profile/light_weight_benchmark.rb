$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), "..", "lib"))

require 'mongo'
require 'benchmark'
require_relative 'benchmark_helper'


# Perform 'lightweight' benchmarks. This includes:
#
#   Run command
#   Find one by ID
#   Small doc insertOne
#   Large doc insertOne
#
# The lightweight benchmark is intended to measure single-document insertion and query tasks,
# to explore basic wire protocol efficiency.
#
# @param [ Integer ] benchmark_reps Number of repetitions of each benchmark to run.
#
# @return [ Array< Array<Integer> > ] Arrays of benchmark results
#
# @since 2.2.1
def lightweight_benchmark(benchmark_reps)
  #bench_helper = BenchmarkHelper.new('perftest','corpus')
  bench_helper = BenchmarkHelper.new('foo','bar')
  database = bench_helper.database
  collection = bench_helper.collection

  results = []
  results << run_command(database, benchmark_reps)
  results << find_one_by_id(database, collection, benchmark_reps)
  results << small_doc_insert_one(database, collection, benchmark_reps)
  #results << large_doc_insert_one(database, collection, benchmark_reps)    #TODO: this benchmark takes WAY too long. LARGE_DOC data is too big
  results
end


# Run command
#
# Measure: run isMaster command 10,000 times
#
# @param [ Mongo::Database ] database MongoDB Database.
# @param [ Integer ] reps Number of repetitions of the benchmark to run.
#
# @return [ [Array<Integer>, Double] ] An array of benchmark wall clock time results and data file size in MB (size returned to be consistent with other benchmarks)
#
# @since 2.2.1
def run_command(database, reps)
  tms_results = Benchmark.bm do |bm|
    reps.times do
      bm.report('Lightweight::Run Command') do
        10000.times do
          database.command(:ismaster => 1)
        end
      end
    end
  end

  # Get the real times (wall clock times) from the Benchmark::Tms objects
  return tms_results.map { |result| result.real }, 0.0
end


# Find one by ID
#
# - Drop database
# - Load TWITTER dataset
# - Insert the first 10,000 documents individually into the 'corpus' collection, adding    TODO: I changed this to all the Twitter documents. Can't get MB of data otherwise -- need to get that from the data file size, b/c object sizes vary b/w languages/implementations
#   sequential _id fields to each before upload
#
# Measure: for each of the 10,000 sequential _id numbers, issue a find command     TODO: I changed this to all the Twitter documents. Can't get MB of data otherwise -- need to get that from the data file size, b/c object sizes vary b/w languages/implementations
#          for that _id on the 'corpus' collection and retrieve the single-document result.
#
# @param [ Mongo::Database ] database MongoDB Database.
# @param [ Mongo::Collection ] collection The collection.
# @param [ Integer ] reps Number of repetitions of the benchmark to run.
#
# @return [ [Array<Integer>, Double] ] An array of benchmark wall clock time results and the size of the dataset in MB
#
# @since 2.2.1
def find_one_by_id(database, collection, reps)
  database.drop
  twitter_data, data_file_size = BenchmarkHelper.load_array_from_file('TWITTER')
  twitter_data_size = twitter_data.size

  #10000.times do |i|
  #  next unless (i < twitter_data_size)
  #  twitter_data[i][:_id] = i
  #  collection.insert_one( twitter_data[i] )
  #end

  twitter_data.each_with_index do |doc, index|
    doc[:_id] = index
    collection.insert_one(doc)
  end

  tms_results = Benchmark.bm do |bm|
    reps.times do
      bm.report('Lightweight::Find one by ID') do
        #10000.times do |i|
        #  collection.find(:_id => i).first
        #end

        twitter_data_size.times do |i|
          collection.find(:_id => i).first
        end
      end
    end
  end

  # Get the real times (wall clock times) from the Benchmark::Tms objects
  return tms_results.map { |result| result.real }, data_file_size/1000000.0
end


##
# Small doc insertOne
#
# - Drop database
# - Load SMALL_DOC dataset
#
# Measure: insert the first 10,000 documents individually into the 'corpus' collection
#          using insert_one. DO NOT manually add an _id field.
#
# @param [ Mongo::Database ] database MongoDB Database.
# @param [ Mongo::Collection ] collection The collection.
# @param [ Integer ] reps Number of repetitions of the benchmark to run.
#
# @return [ [Array<Integer>, Double] ] An array of benchmark wall clock time results and the size of the dataset in MB
#
# @since 2.2.1
def small_doc_insert_one(database, collection, reps)
  small_doc_data, data_file_size = BenchmarkHelper.load_array_from_file('SMALL_DOC')
  small_doc_data_size = small_doc_data.size

  tms_results = []
  reps.times do
    database.drop
    tms_results << Benchmark.bm do |bm|
      bm.report('Lightweight::Small doc insertOne') do
        10000.times do |i|
          collection.insert_one( small_doc_data[i] ) if (i < small_doc_data_size)
        end
      end
    end.first
  end

  # Get the real times (wall clock times) from the Benchmark::Tms objects
  return tms_results.map { |result| result.real }, data_file_size/1000000.0
end


# Large doc insertOne
#
# - Drop database
# - Load LARGE_DOC dataset
#
# Measure: insert the first 1,000 documents individually into the 'corpus' collection
#          using insert_one. DO NOT manually add an _id field.
#
# @param [ Mongo::Database ] database MongoDB Database.
# @param [ Mongo::Collection ] collection The collection.
# @param [ Integer ] reps Number of repetitions of the benchmark to run.
#
# @return [ [Array<Integer>, Double] ] An array of benchmark wall clock time results and the size of the dataset in MB
#
# @since 2.2.1
def large_doc_insert_one(database, collection, reps)
  large_doc_data, data_file_size = BenchmarkHelper.load_array_from_file('LARGE_DOC')
  large_doc_data_size = large_doc_data.size

  tms_results = []
  reps.times do
    database.drop
    tms_results << Benchmark.bm do |bm|
      bm.report('Lightweight::Large doc insertOne') do
        1000.times do |i|
          next unless (i < large_doc_data_size)
          collection.insert_one( large_doc_data[i] )
        end
      end
    end.first
  end

  # Get the real times (wall clock times) from the Benchmark::Tms objects
  return tms_results.map { |result| result.real }, data_file_size/1000000.0
end

#lightweight_benchmark(1)