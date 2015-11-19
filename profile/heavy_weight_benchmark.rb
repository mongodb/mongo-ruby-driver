$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), "..", "lib"))

require 'mongo'
require 'benchmark'
require_relative 'benchmark_helper'
require 'thread'

##
# Perform 'heavyweight' benchmarks. This includes
#
# LDJSON multi-file import
# LDJSON multi-file export
# GridFS multi-file upload
# GridFS multi-file download
#
##
def heavyweight_benchmark!

  # The heavyweight benchmark is intended to test concurrency performance,
  # so increase allowed DB connections
  #bench_helper = BenchmarkHelper.new('perftest','corpus', 200)
  bench_helper = BenchmarkHelper.new('foo','bar', 200)
  database = bench_helper.database
  collection = bench_helper.collection
  print "\n\n\n"



  ##
  # LDJSON multi-file import
  #
  # Dataset: LDJSON_MULTI
  #
  # - Drop the 'perftest' database.
  # - Drop the 'corpus' collection.
  # - Construct whatever objects, threads, etc. are required for importing the dataset but
  #   do not read any data from disk.
  #
  # Measure: Do an unordered insert of all 1,000,000 documents in the dataset into the 'corpus'
  #          collection as fast as possible.  Data must be loaded from disk during this phase.
  #          Concurrency is encouraged.
  #
  # - Drop the 'perftest' database
  ##
  database.drop
  collection.drop
  ldjson_multi_files = Array.new(100) {|i| "LDJSON%02d.txt" % (i+1) }

  first = Benchmark.bmbm do |bm|
    bm.report('Heavyweight::LDJSON multi-file import') do

      threads = []
      ldjson_multi_files.each do |file_name|
        threads << Thread.new do
          ldjson_multi_data = BenchmarkHelper.load_array_from_file(file_name)
          collection.insert_many(ldjson_multi_data, ordered: false)
        end
      end

      threads.each { |t| t.join }
    end
  end

  database.drop
  print "\n\n\n"



  ##
  # LDJSON multi-file export
  #
  # - Drop the 'perftest' database.
  # - Drop the 'corpus' collection.
  # - Do an unordered insert of all 1,000,000 documents in the dataset into the 'corpus' collection.
  #
  # Measure: Dump all 1,000,000 documents in the dataset into 100 LDJSON files of 10,000 documents
  #          each as fast as possible.  Data must be completely written/flushed to disk during this
  #          phase.  Concurrency is encouraged.
  #
  # - Drop the 'perftest' database.
  ##
  database.drop
  collection.drop

  # Array of file names from which to read data, and tmp file names to which to write data
  ldjson_multi_files = Array.new(100) {|i| "LDJSON%03d.txt" % (i+1) }
  ldjson_multi_files_tmp = Array.new(100) {|i| "TMP_LDJSON%03d.txt" % (i+1) }

  ldjson_multi_files.each do |file_name|
    ldjson_multi_data = BenchmarkHelper.load_array_from_file(file_name)
    collection.insert_many(ldjson_multi_data, ordered: false)
  end

  ldjson_tmp_directory = "benchmark_tmp_directory_ldjson"
  BenchmarkHelper.make_directory(ldjson_tmp_directory)

  second = Benchmark.bmbm do |bm|
    bm.report('Heavyweight::LDJSON multi-file export') do
      threads = []
      all_data = collection.find.to_a

      ldjson_multi_files_tmp.each_with_index do |file_name, index|
        data_partition = all_data[index*10000, index*10000 + 10000-1]
        threads << Thread.new do
          BenchmarkHelper.write_documents_to_file("#{ldjson_tmp_directory}/#{file_name}", data_partition)
        end
      end

      threads.each { |t| t.join }
    end
  end

  FileUtils.remove_dir(ldjson_tmp_directory)
  database.drop
  print "\n\n\n"



  ##
  # GridFS multi-file upload
  #
  # - Drop the 'perftest' database.
  # - Drop the default GridFS bucket.  Construct a GridFSBucket object to use for uploads.
  #
  # Measure: Upload all 100 files in the GRIDFS_MULTI dataset (reading each from disk).
  #          Concurrency is encouraged.
  #
  # - Drop the 'perftest' database.
  ##
  database.drop

  gridfs_multi_files = Array.new(100) {|i| "GridFS%03d.txt" % (i+1) }

  third = Benchmark.bmbm do |bm|
    bm.report('Heavyweight::GridFS multi-file upload') do
      threads = []
      gridfs_multi_files.each do |file_name|
        threads << Thread.new do
          gridfs_multi_data = BenchmarkHelper.load_string_from_file(file_name)
          database.fs.insert_one(
              Mongo::Grid::File.new(gridfs_multi_data, :filename => file_name)
          )
        end
      end
      threads.each { |t| t.join }
    end
  end

  database.drop
  print "\n\n\n"



  ##
  # GridFS multi-file download
  #
  # - Drop the 'perftest' database.
  # - Construct a temporary directory for holding downloads.
  # - Drop the default GridFS bucket.
  # - Construct a GridFSBucket object to use for downloads.
  # - Delete all files in the temporary folder for downloads.
  #
  # Measure: Download all 100 files in the GRIDFS_MULTI dataset, saving each to a file
  #          in the temporary folder for downloads. Data must be completely written/flushed
  #          to disk during this phase.  Concurrency is encouraged.
  #
  # - Drop the 'perftest' database.
  ##
  database.drop

  gridfs_tmp_directory = "benchmark_tmp_directory_gridfs"
  BenchmarkHelper.make_directory(gridfs_tmp_directory)


  # Array of file names from which to read data, and tmp file names to which to write data
  gridfs_multi_files = Array.new(100) {|i| "GridFS%03d.txt" % (i+1) }
  gridfs_multi_files_tmp = Array.new(100) {|i| "TMP_GridFS%03d.txt" % (i+1) }

  gridfs_multi_files.each do |file_name|
    gridfs_multi_data = BenchmarkHelper.load_string_from_file(file_name)
    database.fs.insert_one(
        Mongo::Grid::File.new(gridfs_multi_data, :filename => file_name)
    )
  end

  fourth = Benchmark.bmbm do |bm|
    bm.report('Heavyweight::GridFS multi-file download') do

      threads = []
      gridfs_multi_files.each_with_index do |file_name, index|
        threads << Thread.new do

          file_ptr = File.open("#{gridfs_tmp_directory}/TMP_GridFS%03d.txt" % (index+1), 'w')
          database.fs.download_to_stream_by_name(file_name, file_ptr)
        end
      end
      threads.each { |t| t.join }
    end
  end

  FileUtils.remove_dir(gridfs_tmp_directory)
  database.drop
  print "\n\n\n"
end