$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), "..", "lib"))

require 'mongo'
require 'benchmark'
require_relative 'benchmark_helper'

SMALL_DOC = "SMALL_DOC"
LARGE_DOC = "LARGE_DOC"
GRIDFS_LARGE = "GRIDFS_LARGE"
GRIDFSTEST = "gridfstest"

# Perform 'middleweight' benchmarks. This includes
#
#   Find many and empty the cursor
#   Small doc bulk insert
#   Large doc bulk insert
#   GridFS upload
#   GridFS download
#
# The featherweight benchmark is intended to measure multi-document insertion and query tasks,
# to explore batch-write and cursor chunking efficiency.
#
# @param [ Integer ] benchmark_reps Number of repetitions of each benchmark to run.
#
# @return [ Array< Array<Integer> > ] Arrays of benchmark results
#
# @since 2.2.1
def middleweight_benchmark(benchmark_reps)
  #bench_helper = BenchmarkHelper.new('perftest','corpus')
  bench_helper = BenchmarkHelper.new('foo','bar')
  database = bench_helper.database
  collection = bench_helper.collection

  results = []
  results << find_many_and_empty_the_cursor(database, collection, benchmark_reps)
  results << small_doc_bulk_insert(database, collection, benchmark_reps)
  #results << large_doc_bulk_insert(database, collection, benchmark_reps)    #TODO: this benchmark takes WAY too long. LARGE_DOC data is too big
  results << gridfs_upload(database, benchmark_reps)
  results << gridfs_download(database, benchmark_reps)
  results
end


# Find many and empty the cursor
#
# - Drop the database
# - Load TWITTER dataset
# - Insert the first 10,000 documents into the 'corpus' collection.  TODO: I changed this to all the Twitter documents. Can't get MB of data otherwise -- need to get that from the data file size, b/c object sizes vary b/w languages/implementations
#
# Measure: Issue a find command on the 'corpus' collection with an empty filter expression.
#          Retrieve (and discard) all documents from the cursor.
#
# @param [ Mongo::Database ] database MongoDB Database.
# @param [ Mongo::Collection ] collection The collection.
# @param [ Integer ] reps Number of repetitions of the benchmark to run.
#
# @return [ [Array<Integer>, Double] ] An array of benchmark wall clock time results and the size of the dataset in MB
#
# @since 2.2.1
def find_many_and_empty_the_cursor(database, collection, reps)
  database.drop
  twitter_data, data_file_size = BenchmarkHelper.load_array_from_file('TWITTER')
  #twitter_data_size = twitter_data.size

  #10000.times do |i|
  #  collection.insert_one( twitter_data[i] ) if (i < twitter_data_size)
  #end

  twitter_data.each do |twitter_doc|
    collection.insert_one(twitter_doc)
  end

  tms_results = Benchmark.bm do |bm|
    reps.times do
      bm.report('Middleweight::Find many and empty the cursor') do
        collection.find.to_a
      end
    end
  end

  # Get the real times (wall clock times) from the Benchmark::Tms objects
  return tms_results.map { |result| result.real }, data_file_size/1000000.0
end


# Small doc bulk insert
#
# - Drop the database.
# - Load SMALL_DOC dataset
#
# Measure: Do an ordered bulk insert of all 10,000 documents.
#          DO NOT manually add an _id field
#
# @param [ Mongo::Database ] database MongoDB Database.
# @param [ Mongo::Collection ] collection The collection.
# @param [ Integer ] reps Number of repetitions of the benchmark to run.
#
# @return [ [Array<Integer>, Double] ] An array of benchmark wall clock time results and the size of the dataset in MB
#
# @since 2.2.1
def small_doc_bulk_insert(database, collection, reps)
  small_doc_data, data_file_size = BenchmarkHelper.load_array_from_file(SMALL_DOC)

  tms_results = []
  reps.times do
    database.drop
    tms_results << Benchmark.bm do |bm|
      bm.report('Middleweight::Small doc bulk insert') do
        collection.insert_many(small_doc_data, ordered: true)
      end
    end.first
  end

  # Get the real times (wall clock times) from the Benchmark::Tms objects
  return tms_results.map { |result| result.real }, data_file_size/1000000.0
end


# Large doc bulk insert
#
# - Drop the database.
# - Load LARGE_DOC dataset
#
# Measure: Do an ordered bulk insert of all 1,000 documents.
#          DO NOT manually add an _id field
#
# @param [ Mongo::Database ] database MongoDB Database.
# @param [ Mongo::Collection ] collection The collection.
# @param [ Integer ] reps Number of repetitions of the benchmark to run.
#
# @return [ [Array<Integer>, Double] ] An array of benchmark wall clock time results and the size of the dataset in MB
#
# @since 2.2.1
def large_doc_bulk_insert(database, collection, reps)
  large_doc_data, data_file_size = BenchmarkHelper.load_array_from_file(LARGE_DOC)

  tms_results = []
  reps.times do
    database.drop
    tms_results = Benchmark.bm do |bm|
      bm.report('Middleweight::Large doc bulk insert') do
        collection.insert_many(large_doc_data, ordered: true)
      end
    end
  end

  # Get the real times (wall clock times) from the Benchmark::Tms objects
  return tms_results.map { |result| result.real }, data_file_size/1000000.0
end


##
# GridFS upload
#
# - Drop the database.
# - Load the GRIDFS_LARGE file as a string.
#
# Measure: Upload the GRIDFS_LARGE data as a GridFS file 100 times.  Each time, use a
#          different filename, following the sprintf pattern "gridfstest%03d" with integers
#          from 1 to 100.
#
# @param [ Mongo::Database ] database MongoDB Database.
# @param [ Integer ] reps Number of repetitions of the benchmark to run.
#
# @return [ [Array<Integer>, Double] ] An array of benchmark wall clock time results and the size of the dataset in MB
#
# @since 2.2.1
def gridfs_upload(database, reps)
  gridfs_large_data_string, data_file_size = BenchmarkHelper.load_string_from_file(GRIDFS_LARGE)

  tms_results = []
  reps.times do
    database.drop
    tms_results << Benchmark.bm do |bm|
      bm.report('Middleweight::GridFS upload') do
        100.times do |i|
          database.fs.insert_one(
              Mongo::Grid::File.new(gridfs_large_data_string, :filename => "gridfstest%03d" % (i+1))
          )
        end
      end
    end.first
  end

  # Get the real times (wall clock times) from the Benchmark::Tms objects
  return tms_results.map { |result| result.real }, data_file_size*100/1000000.0
end


# GridFS download
#
# - Drop the database
# - Upload the GRIDFS_LARGE file to the default gridFS bucket with the name "gridfstest".
#   Record the _id of the uploaded file.
#
# Measure: Download the "gridfstest" file by its _id 100 times.  Use whatever download API
#         is most natural for each language (e.g. open_download_stream(), read from the
#         stream into a variable).  Discard the downloaded data after each download.
#
# @param [ Mongo::Database ] database MongoDB Database.
# @param [ Integer ] reps Number of repetitions of the benchmark to run.
#
# @return [ [Array<Integer>, Double] ] An array of benchmark wall clock time results and the size of the dataset in MB
#
# @since 2.2.1
def gridfs_download(database, reps)
  database.drop
  gridfs_large_data_string, data_file_size = BenchmarkHelper.load_string_from_file(GRIDFS_LARGE)
  gridfs_file = Mongo::Grid::File.new( gridfs_large_data_string, :filename => GRIDFSTEST )
  database.fs.insert_one( gridfs_file )

  tms_results = Benchmark.bm do |bm|
    reps.times do
      bm.report('Middleweight::GridFS download') do
        100.times do
          stream = StringIO.new
          database.fs.download_to_stream(gridfs_file.id, stream)
        end
      end
    end
  end

  # Get the real times (wall clock times) from the Benchmark::Tms objects
  return tms_results.map { |result| result.real }, data_file_size*100/1000000.0
end

#middleweight_benchmark(1)

