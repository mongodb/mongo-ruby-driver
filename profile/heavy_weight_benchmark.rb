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
# The heavyweight benchmark is intended to test multi-process/thread ETL tasks,
# to explore concurrent operation efficiency.
#
##
def heavyweight_benchmark!
  #bench_helper = BenchmarkHelper.new('perftest','corpus', 200)
  bench_helper = BenchmarkHelper.new('foo','bar', 200)
  database = bench_helper.database
  collection = bench_helper.collection



  ##
  # LDJSON multi-file import
  #
  # Dataset: LDJSON_MULTI
  #
  # - Drop the 'perftest' database.
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

  # The directory name, or path to the directory, in which the LDJSON files are expected to be
  ldjson_data_files_directory = "LDJSON_data_file_directory"
  # Array of expected file names
  ldjson_multi_files = Array.new(100) {|i| "LDJSON%03d.txt" % (i+1) }

  first = Benchmark.bm do |bm|
    bm.report('Heavyweight::LDJSON multi-file import') do

      threads = []
      ldjson_multi_files.each do |file_name|
        threads << Thread.new do
          ldjson_multi_data = BenchmarkHelper.load_array_from_file("#{ldjson_data_files_directory}/#{file_name}")
          collection.insert_many(ldjson_multi_data, ordered: false)
        end
      end

      threads.each { |t| t.join }
    end
  end

  database.drop



  ##
  # LDJSON multi-file export
  #
  # - Drop the 'perftest' database.
  # - Do an unordered insert of all 1,000,000 documents in the dataset into the 'corpus' collection.
  #
  # Measure: Dump all 1,000,000 documents in the dataset into 100 LDJSON files of 10,000 documents
  #          each as fast as possible.  Data must be completely written/flushed to disk during this
  #          phase.  Concurrency is encouraged.
  #
  # - Drop the 'perftest' database.
  ##
  database.drop

  # Temporary directory in which to store temporary data files into which document data will be dumped
  ldjson_data_files_tmp_directory = "LDJSON_data_file_tmp_directory"
  BenchmarkHelper.make_directory(ldjson_data_files_tmp_directory)

  # The directory name, or path to the directory, in which the LDJSON files are expected to be
  ldjson_data_files_directory = "LDJSON_data_file_directory"
  # Array of expected file names
  ldjson_multi_files = Array.new(100) {|i| "LDJSON%03d.txt" % (i+1) }
  # Array of temporary file names to which to dump document data
  ldjson_multi_files_tmp = Array.new(100) {|i| "TMP_LDJSON%03d.txt" % (i+1) }

  ldjson_multi_files.each do |file_name|
    ldjson_multi_data = BenchmarkHelper.load_array_from_file("#{ldjson_data_files_directory}/#{file_name}")
    collection.insert_many(ldjson_multi_data, ordered: false)
  end

  second = Benchmark.bm do |bm|
    bm.report('Heavyweight::LDJSON multi-file export') do
      all_data = collection.find.to_a

      threads = []
      ldjson_multi_files_tmp.each_with_index do |file_name, index|
        data_partition = all_data[index*10000, index*10000 + 10000-1]
        threads << Thread.new do
          BenchmarkHelper.write_documents_to_file("#{ldjson_data_files_tmp_directory}/#{file_name}", data_partition)
        end
      end

      threads.each { |t| t.join }
    end
  end

  FileUtils.remove_dir(ldjson_data_files_tmp_directory)
  database.drop



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

  # The directory name, or path to the directory, in which the GridFS files are expected to be
  gridfs_data_files_directory = "GridFS_data_file_directory"
  # Array of expected file names
  gridfs_multi_files = Array.new(100) {|i| "GridFS%03d.txt" % (i+1) }

  third = Benchmark.bm do |bm|
    bm.report('Heavyweight::GridFS multi-file upload') do

      threads = []
      gridfs_multi_files.each do |file_name|
        threads << Thread.new do
          gridfs_multi_data = BenchmarkHelper.load_string_from_file("#{gridfs_data_files_directory}/#{file_name}")
          database.fs.insert_one(
              Mongo::Grid::File.new(gridfs_multi_data, :filename => file_name)
          )
        end
      end

      threads.each { |t| t.join }
    end
  end

  database.drop



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

  # Temporary directory in which to store temporary data files into which document data will be dumped
  gridfs_data_files_tmp_directory = "GridFS_data_file_tmp_directory"
  BenchmarkHelper.make_directory(gridfs_data_files_tmp_directory)

  # The directory name, or path to the directory, in which the GridFS files are expected to be
  gridfs_data_files_directory = "GridFS_data_file_directory"
  # Array of expected file names
  gridfs_multi_files = Array.new(100) {|i| "GridFS%03d.txt" % (i+1) }

  # Load GridFS datasets into the DB.
  gridfs_multi_files.each do |file_name|
    gridfs_multi_data = BenchmarkHelper.load_string_from_file("#{gridfs_data_files_directory}/#{file_name}")
    database.fs.insert_one(
        Mongo::Grid::File.new(gridfs_multi_data, :filename => file_name)
    )
  end

  fourth = Benchmark.bm do |bm|
    bm.report('Heavyweight::GridFS multi-file download') do

      threads = []
      gridfs_multi_files.each_with_index do |file_name, index|
        threads << Thread.new do
          file_ptr = File.open("#{gridfs_data_files_tmp_directory}/TMP_GridFS%03d.txt" % (index+1), 'w')
          database.fs.download_to_stream_by_name(file_name, file_ptr)
        end
      end

      threads.each { |t| t.join }
    end
  end

  FileUtils.remove_dir(gridfs_data_files_tmp_directory)
  database.drop


  first_results = first.map {|res| res.real}
  second_results = second.map {|res| res.real}
  third_results = third.map {|res| res.real}
  fourth_results = fourth.map {|res| res.real}
  return first_results, second_results, third_results, fourth_results
end