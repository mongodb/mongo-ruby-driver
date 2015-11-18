$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), "..", "lib"))

require 'mongo'
require 'benchmark'
require_relative 'benchmark_helper'

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

  #bench_helper = BenchmarkHelper.new('perftest','corpus')
  bench_helper = BenchmarkHelper.new('foo','bar')
  database = bench_helper.database
  collection = bench_helper.collection
  print "\n\n\n"



  ##
  # LDJSON multi-file import
  #
  # - Drop the 'perftest' database.
  # - Drop the 'corpus' collection.
  # - Load LDJSON_MULTI dataset. Construct whatever objects, threads, etc. are
  # required for importing the dataset but do not read any data from disk.
  #
  # Measure: Do an unordered insert of all 1,000,000 documents in the dataset
  #          into the 'corpus' collection as fast as possible.  Data must be
  #          loaded from disk during this phase.  Concurrency is encouraged.
  #
  # - Drop the 'perftest' database
  ##
  first = Benchmark.bmbm do |bm|
    bm.report('Heavyweight::LDJSON multi-file import') do

    end
  end
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
  second = Benchmark.bmbm do |bm|
    bm.report('Heavyweight::LDJSON multi-file export') do

    end
  end
  print "\n\n\n"



  ##
  # GridFS multi-file upload
  #
  # - Drop the 'perftest' database.
  # -  Drop the default GridFS bucket.  Construct a GridFSBucket object to use for uploads.
  #
  # Measure: Upload all 100 files in the GRIDFS_MULTI dataset (reading each from disk).
  #          Concurrency is encouraged.
  #
  # - Drop the 'perftest' database.
  ##
  third = Benchmark.bmbm do |bm|
    bm.report('Heavyweight::GridFS multi-file upload') do

    end
  end
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
  fourth = Benchmark.bmbm do |bm|
    bm.report('Heavyweight::GridFS multi-file download') do

    end
  end
  print "\n\n\n"
end