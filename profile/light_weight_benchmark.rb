$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), "..", "lib"))

require 'mongo'
require 'benchmark'
require_relative 'benchmark_helper'

TWITTER = "TWITTER"
SMALL_DOC = "SMALL_DOC"
LARGE_DOC = "LARGE_DOC"
BYTE_SIZE_IS_MASTER_COMMAND = 19

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
  bench_helper = BenchmarkHelper.new('perftest','corpus')
  database = bench_helper.database
  collection = bench_helper.collection

  results = []
  results << run_command(database, benchmark_reps)
  results << find_one_by_id(database, collection, benchmark_reps)
  results << small_doc_insert_one(database, collection, benchmark_reps)
  #results << large_doc_insert_one(database, collection, benchmark_reps)    #TODO: this benchmark takes WAY too long. LARGE_DOC data is too big
end


# Run command
#
# Measure: run isMaster command 10,000 times
#
# @param [ Mongo::Database ] database MongoDB Database.
# @param [ Integer ] reps Number of repetitions of the benchmark to run.
#
# @return [ [Array<Integer>, Double, String] ] An array of benchmark wall clock time results,
#                                              the size of the dataset in MB, test label
# @since 2.2.1
def run_command(database, reps)
  tms_results = Benchmark.bm do |bm|
    reps.times do
      bm.report('Lightweight::Run Command') do
        450000.times do # This is set higher than 10,000 so that it runs for at least a minute
          database.command(:ismaster => 1)
        end
      end
    end
  end

  # Get the real times (wall clock times) from the Benchmark::Tms objects
  return tms_results.map { |result| result.real }, BYTE_SIZE_IS_MASTER_COMMAND*450000, "Run command"
end


# Find one by ID
#
# - Drop database
# - Load TWITTER dataset
# - Insert the first 10,000 documents individually into the 'corpus' collection, adding    #TODO:QUESTION I changed this to all the Twitter documents. Can't get MB of data otherwise -- need to get that from the data file size, b/c object sizes vary b/w languages/implementations
#   sequential _id fields to each before upload
#
# Measure: for each of the 10,000 sequential _id numbers, issue a find command     #TODO:QUESTION I changed this to all the Twitter documents. Can't get MB of data otherwise -- need to get that from the data file size, b/c object sizes vary b/w languages/implementations
#          for that _id on the 'corpus' collection and retrieve the single-document result.
#
# @param [ Mongo::Database ] database MongoDB Database.
# @param [ Mongo::Collection ] collection The collection.
# @param [ Integer ] reps Number of repetitions of the benchmark to run.
#
# @return [ [Array<Integer>, Double, String] ] An array of benchmark wall clock time results,
#                                              the size of the dataset in MB, test label
# @since 2.2.1
def find_one_by_id(database, collection, reps)
  database.drop
  twitter_data = BenchmarkHelper.load_array_from_file(TWITTER)
  data_file_size = File.size(TWITTER)
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
  return tms_results.map { |result| result.real }, data_file_size/1000000.0, "Find one by ID"
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
# @return [ [Array<Integer>, Double, String] ] An array of benchmark wall clock time results,
#                                              the size of the dataset in MB, test label
# @since 2.2.1
def small_doc_insert_one(database, collection, reps)
  small_doc_data = BenchmarkHelper.load_array_from_file(SMALL_DOC)
  data_file_size = File.size(SMALL_DOC)

  tms_results = []
  reps.times do
    database.drop
    tms_results << Benchmark.bm do |bm|
      bm.report('Lightweight::Small doc insertOne') do  # Changing this from 10,000 to entire file so that data size can be determined
        small_doc_data.each do |doc|
          collection.insert_one( doc )
        end
      end
    end.first
  end

  # Get the real times (wall clock times) from the Benchmark::Tms objects
  return tms_results.map { |result| result.real }, data_file_size/1000000.0, "Small doc insert one"
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
# @return [ [Array<Integer>, Double, String] ] An array of benchmark wall clock time results,
#                                              the size of the dataset in MB, test label
# @since 2.2.1
def large_doc_insert_one(database, collection, reps)
  large_doc_data = BenchmarkHelper.load_array_from_file(LARGE_DOC)
  data_file_size = File.size(LARGE_DOC)

  tms_results = []
  reps.times do
    database.drop
    tms_results << Benchmark.bm do |bm|
      bm.report('Lightweight::Large doc insertOne') do  # Changing this from 1,000 to entire file so that data size can be determined
        large_doc_data.each do |doc|
          collection.insert_one( doc )
        end
      end
    end.first
  end

  # Get the real times (wall clock times) from the Benchmark::Tms objects
  return tms_results.map { |result| result.real }, data_file_size/1000000.0, "Large doc insert one"
end
