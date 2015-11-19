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

  # NOTE: #bmbm does a rehearsal and then the test, so it performs the action twice. This seems like a problem
  # TODO: don't use #bmbm in any of the benchmarks, in feather, light, middle, heavy

  # TODO: make sure the tests are robust enough that not having enough documents handed in by the dataset doesn't break anything. Double check all benchmark files' tests

  #bench_helper = BenchmarkHelper.new('perftest','corpus')
  bench_helper = BenchmarkHelper.new('foo','bar')
  database = bench_helper.database
  collection = bench_helper.collection
  print "\n\n\n"



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

  5.times do |i|
    next unless (i < twitter_data_size)
    twitter_data[i][:_id] = i
    collection.insert_one( twitter_data[i] )
  end

  first = Benchmark.bmbm do |bm|
    bm.report('Middleweight::Find many and empty the cursor') do
      collection.find.to_a
    end
  end

  database.drop
  print "\n\n\n"



  ##
  # Small doc bulk insert
  #
  # - Drop the 'perftest' database.
  # - Load SMALL_DOC dataset
  # - Drop the 'corpus' collection.
  #
  # Measure: Do an ordered bulk insert of all 10,000 documents.
  #          DO NOT manually add an _id field
  #
  # - Drop the 'perftest' database.
  ##
  database.drop
  small_doc_data = BenchmarkHelper.load_array_from_file('SMALL_DOC.txt')
  collection.drop

  second = Benchmark.bmbm do |bm|
    bm.report('Middleweight::Small doc bulk insert') do
      collection.insert_many(small_doc_data, ordered: true)
    end
  end

  database.drop
  print "\n\n\n"



  ##
  # Large doc bulk insert
  #
  # - Drop the 'perftest' database.
  # - Load LARGE_DOC dataset
  # - Drop the 'corpus' collection.
  #
  # Measure: Do an ordered bulk insert of all 1,000 documents.
  #          DO NOT manually add an _id field
  #
  # - Drop the 'perftest' database.
  ##
  database.drop
  large_doc_data = BenchmarkHelper.load_array_from_file('LARGE_DOC.txt')
  collection.drop

  third = Benchmark.bmbm do |bm|
    bm.report('Middleweight::Large doc bulk insert') do
      collection.insert_many(large_doc_data, ordered: true)
    end
  end

  database.drop
  print "\n\n\n"



  ##
  # GridFS upload
  #
  # - Drop the 'perftest' database.
  # - Load the GRIDFS_LARGE file as a string.
  #
  # Measure: Upload the GRIDFS_LARGE data as a GridFS file 100 times.  Each time, use a
  #          different filename, following the sprintf pattern "gridfstest%03d" with integers
  #          from 1 to 100.  Use whatever upload API is most natural for each language
  #          (e.g. open_upload_stream(), write the data to the stream and close the stream).
  #
  # - Drop the 'perftest' database.
  ##
  database.drop
  gridfs_large_data_string = BenchmarkHelper.load_string_from_file('GRIDFS_LARGE.txt')

  fourth = Benchmark.bmbm do |bm|
    bm.report('Middleweight::GridFS upload') do
      100.times do |i|
        file_name = "gridfstest%03d" % (i+1)
        database.fs.insert_one(
            Mongo::Grid::File.new(gridfs_large_data_string, :filename => file_name)
        )
      end
    end
  end

  database.drop
  print "\n\n\n"



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

  fifth = Benchmark.bmbm do |bm|
    bm.report('Middleweight::GridFS download') do
      100.times do
        database.fs.find_one(:_id => gridfs_file.id)
      end
    end
  end

  database.drop
  print "\n\n\n"
end
