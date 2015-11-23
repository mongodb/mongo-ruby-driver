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
# The featherweight benchmark is intended to measure multi-document insertion and query tasks,
# to explore batch-write and cursor chunking efficiency.
#
##
def middleweight_benchmark!
  #bench_helper = BenchmarkHelper.new('perftest','corpus')
  bench_helper = BenchmarkHelper.new('foo','bar')
  database = bench_helper.database
  collection = bench_helper.collection



  ##
  # Find many and empty the cursor
  #
  # - Drop the 'perftest' database
  # - Load TWITTER dataset
  # - Insert the first 10,000 documents into the 'corpus' collection.
  #
  # Measure: Issue a find command on the 'corpus' collection with an empty filter expression.
  #          Retrieve (and discard) all documents from the cursor.
  #
  # - Drop the 'perftest' database
  ##
  database.drop
  twitter_data = BenchmarkHelper.load_array_from_file('TWITTER.txt')
  twitter_data_size = twitter_data.size

  10000.times do |i|
    collection.insert_one( twitter_data[i] ) if (i < twitter_data_size)
  end

  first = Benchmark.bm do |bm|
    bm.report('Middleweight::Find many and empty the cursor') do
      collection.find.to_a
    end
  end

  database.drop



  ##
  # Small doc bulk insert
  #
  # - Drop the 'perftest' database.
  # - Load SMALL_DOC dataset
  #
  # Measure: Do an ordered bulk insert of all 10,000 documents.
  #          DO NOT manually add an _id field
  #
  # - Drop the 'perftest' database.
  ##
  database.drop
  small_doc_data = BenchmarkHelper.load_array_from_file('SMALL_DOC.txt')

  second = Benchmark.bm do |bm|
    bm.report('Middleweight::Small doc bulk insert') do
      collection.insert_many(small_doc_data, ordered: true)
    end
  end

  database.drop



  ##
  # Large doc bulk insert
  #
  # - Drop the 'perftest' database.
  # - Load LARGE_DOC dataset
  #
  # Measure: Do an ordered bulk insert of all 1,000 documents.
  #          DO NOT manually add an _id field
  #
  # - Drop the 'perftest' database.
  ##
  database.drop
  large_doc_data = BenchmarkHelper.load_array_from_file('LARGE_DOC.txt')

  third = Benchmark.bm do |bm|
    bm.report('Middleweight::Large doc bulk insert') do
      collection.insert_many(large_doc_data, ordered: true)
    end
  end

  database.drop



  ##
  # GridFS upload
  #
  # - Drop the 'perftest' database.
  # - Load the GRIDFS_LARGE file as a string.
  #
  # Measure: Upload the GRIDFS_LARGE data as a GridFS file 100 times.  Each time, use a
  #          different filename, following the sprintf pattern "gridfstest%03d" with integers
  #          from 1 to 100.
  #
  # - Drop the 'perftest' database.
  ##
  database.drop
  gridfs_large_data_string = BenchmarkHelper.load_string_from_file('GRIDFS_LARGE.txt')

  fourth = Benchmark.bm do |bm|
    bm.report('Middleweight::GridFS upload') do
      100.times do |i|
        database.fs.insert_one(
            Mongo::Grid::File.new(gridfs_large_data_string, :filename => "gridfstest%03d" % (i+1))
        )
      end
    end
  end

  database.drop



  ##
  # GridFS download
  #
  # - Drop the 'perftest' database
  # - Upload the GRIDFS_LARGE file to the default gridFS bucket with the name "gridfstest".
  #   Record the _id of the uploaded file.
  # - Construct a GridFSBucket object to use for downloads.
  #
  # Measure: Download the "gridfstest" file by its _id 100 times.  Use whatever download API
  #         is most natural for each language (e.g. open_download_stream(), read from the
  #         stream into a variable).  Discard the downloaded data after each download.
  #
  # - Drop the 'perftest' database
  ##
  database.drop
  gridfs_large_data_string = BenchmarkHelper.load_string_from_file('GRIDFS_LARGE.txt')
  gridfs_file = Mongo::Grid::File.new( gridfs_large_data_string, :filename => "gridfstest" )
  database.fs.insert_one( gridfs_file )

  fifth = Benchmark.bm do |bm|
    bm.report('Middleweight::GridFS download') do
      100.times do
        stream = StringIO.new
        database.fs.download_to_stream(gridfs_file.id, stream)
      end
    end
  end

  database.drop



  first_results = first.map {|res| res.real}
  second_results = second.map {|res| res.real}
  third_results = third.map {|res| res.real}
  fourth_results = fourth.map {|res| res.real}
  fifth_results = fifth.map {|res| res.real}
  return first_results, second_results, third_results, fourth_results, fifth_results
end
