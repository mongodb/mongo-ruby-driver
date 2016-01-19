$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), "..", "lib"))

require 'mongo'
require 'benchmark'
require_relative 'benchmark_helper'
require 'thread'
require 'bson'

# The directory name in which the LDJSON files are expected to be
LDJSON_DIR = "LDJSON_data_file_directory"
# Temporary directory in which to write LDJSON data
LDJSON_TMP_DIR = "LDJSON_data_file_tmp_directory"
# The directory name in which the GridFS files are expected to be
GRIDFS_DIR = "GridFS_data_file_directory"
# Temporary directory in which to write GridFS data
GRIDFS_TMP_DIR = "GridFS_data_file_tmp_directory"

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
# @param [ Integer ] benchmark_reps Number of repetitions of each benchmark to run.
#
# @return [ Array< Array<Integer> > ] Arrays of benchmark results
#
# @since 2.2.1
def heavyweight_benchmark(benchmark_reps)
  bench_helper = BenchmarkHelper.new('perftest','corpus', 200)
  database = bench_helper.database
  collection = bench_helper.collection

  results = []
  results << ldjson_multi_file_import(database, collection, benchmark_reps)
  results << ldjson_multi_file_export(database, collection, benchmark_reps)
  results << gridfs_multi_file_upload(database, benchmark_reps)
  results << gridfs_multi_file_download(database, benchmark_reps)
end


# LDJSON multi-file import
#
# Dataset: LDJSON_MULTI
#
# - Drop the database.
# - Construct whatever objects, threads, etc. are required for importing the dataset but
#   do not read any data from disk.
#
# Measure: Do an unordered insert of all 1,000,000 documents in the dataset into the 'corpus'
#          collection as fast as possible.  Data must be loaded from disk during this phase.
#          Concurrency is encouraged.
#
# @param [ Mongo::Database ] database MongoDB Database.
# @param [ Mongo::Collection ] collection The collection.
# @param [ Integer ] reps Number of repetitions of the benchmark to run.
#
# @return [ [Array<Integer>, Double, String] ] An array of benchmark wall clock time results,
#                                              the size of the dataset in MB, test label
# @since 2.2.1
def ldjson_multi_file_import(database, collection, reps)
  # Array of expected LDJSON file names
  ldjson_multi_files = Dir["#{LDJSON_DIR}/*"]

  # Get dataset size -- all the datasets are the same size, so get one and multiple by 100
  data_file_size = File.size("#{ldjson_multi_files[0]}") * 100

  tms_results = []
  reps.times do
    database.drop
    tms_results << Benchmark.bm do |bm|
      bm.report('Heavyweight::LDJSON multi-file import') do

        threads = []
        ldjson_multi_files.each do |file_path|
          threads << Thread.new do
            ldjson_multi_data = BenchmarkHelper.load_array_from_file("#{file_path}")
            collection.insert_many(ldjson_multi_data, ordered: false)
          end
        end

        threads.each { |t| t.join }
      end
    end.first
  end

  # Get the real times (wall clock times) from the Benchmark::Tms objects
  return tms_results.map { |result| result.real }, data_file_size/1000000.0, "LDJSON multi-file import"
end


# LDJSON multi-file export
#
# - Drop the database.
# - Do an unordered insert of all 1,000,000 documents in the dataset into the 'corpus' collection.
#
# Measure: Dump all 1,000,000 documents in the dataset into 100 LDJSON files of 10,000 documents
#          each as fast as possible.  Data must be completely written/flushed to disk during this
#          phase.  Concurrency is encouraged.
#
# @param [ Mongo::Database ] database MongoDB Database.
# @param [ Mongo::Collection ] collection The collection.
# @param [ Integer ] reps Number of repetitions of the benchmark to run.
#
# @return [ [Array<Integer>, Double, String] ] An array of benchmark wall clock time results,
#                                              the size of the dataset in MB, test label
# @since 2.2.1
def ldjson_multi_file_export(database, collection, reps)
  database.drop

  # Array of expected LDJSON file names
  ldjson_multi_files =  Dir["#{LDJSON_DIR}/*"]
  # Temporary directory in which to store temporary data files into which document data will be dumped
  ldjson_data_files_tmp_directory = LDJSON_TMP_DIR
  # Array of temporary file names to which to dump document data
  ldjson_multi_files_tmp = Array.new(100) {|i| "TMP_LDJSON%03d.txt" % (i+1) }

  ldjson_multi_files.each do |file_path|
    ldjson_multi_data = BenchmarkHelper.load_array_from_file("#{file_path}")
    collection.insert_many(ldjson_multi_data, ordered: false)
  end

  # Get dataset size -- all the datasets are the same size, so get one and multiple by 100
  data_file_size = File.size("#{ldjson_multi_files[0]}") * 100

  tms_results = []
  reps.times do
    BenchmarkHelper.make_directory(ldjson_data_files_tmp_directory)

    tms_results << Benchmark.bm do |bm|
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
    end.first

    FileUtils.remove_dir(ldjson_data_files_tmp_directory)
  end

  # Get the real times (wall clock times) from the Benchmark::Tms objects
  return tms_results.map { |result| result.real }, data_file_size/1000000.0, "LDJSON multi-file export"
end


# GridFS multi-file upload
#
# - Drop the database.
#
# Measure: Upload all 100 files in the GRIDFS_MULTI dataset (reading each from disk).
#          Concurrency is encouraged.
#
# @param [ Mongo::Database ] database MongoDB Database.
# @param [ Integer ] reps Number of repetitions of the benchmark to run.
#
# @return [ [Array<Integer>, Double, String] ] An array of benchmark wall clock time results,
#                                              the size of the dataset in MB, test label
# @since 2.2.1
def gridfs_multi_file_upload(database, reps)
  # Array of expected GridFS data file names
  gridfs_multi_files = Dir["#{GRIDFS_DIR}/*"]

  # Get dataset size -- all the datasets are the same size, so get one and multiple by 100
  data_file_size = File.size("#{gridfs_multi_files[0]}") * 100

  tms_results = []
  reps.times do
    database.drop
    tms_results << Benchmark.bm do |bm|
      bm.report('Heavyweight::GridFS multi-file upload') do

        threads = []
        gridfs_multi_files.each do |file_path|
          threads << Thread.new do
            gridfs_multi_data = BenchmarkHelper.load_string_from_file("#{file_path}")
            database.fs.insert_one(
                Mongo::Grid::File.new(gridfs_multi_data, :filename => file_path)
            )
          end
        end

        threads.each { |t| t.join }
      end
    end.first
  end

  # Get the real times (wall clock times) from the Benchmark::Tms objects
  return tms_results.map { |result| result.real }, data_file_size/1000000.0, "GridFS multi-file upload"
end


# GridFS multi-file download
#
# - Drop the database.
# - Construct a temporary directory for holding downloads.
#
# Measure: Download all 100 files in the GRIDFS_MULTI dataset, saving each to a file
#          in the temporary folder for downloads. Data must be completely written/flushed
#          to disk during this phase.  Concurrency is encouraged.
#
# @param [ Mongo::Database ] database MongoDB Database.
# @param [ Integer ] reps Number of repetitions of the benchmark to run.
#
# @return [ [Array<Integer>, Double, String] ] An array of benchmark wall clock time results,
#                                              the size of the dataset in MB, test label
# @since 2.2.1
def gridfs_multi_file_download(database, reps)
  database.drop

  # Temporary directory in which to store temporary data files into which document data will be dumped
  gridfs_data_files_tmp_directory = GRIDFS_TMP_DIR
  # Array of expected GridFS data file names
  gridfs_multi_files = Dir["#{GRIDFS_DIR}/*"]

  # Load GridFS datasets into the DB.
  gridfs_multi_files.each do |file_path|
    gridfs_multi_data = BenchmarkHelper.load_string_from_file("#{file_path}")
    database.fs.insert_one(
        Mongo::Grid::File.new(gridfs_multi_data, :filename => file_path)
    )
  end

  # Get dataset size -- all the datasets are the same size, so get one and multiple by 100
  data_file_size = File.size("#{gridfs_multi_files[0]}") * 100

  tms_results = []
  reps.times do
    BenchmarkHelper.make_directory(gridfs_data_files_tmp_directory)

    tms_results << Benchmark.bm do |bm|
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
    end.first

    FileUtils.remove_dir(gridfs_data_files_tmp_directory)
  end

  # Get the real times (wall clock times) from the Benchmark::Tms objects
  return tms_results.map { |result| result.real }, data_file_size/1000000.0, "GridFS multi-file download"
end
