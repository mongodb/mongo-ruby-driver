$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), "..", "lib"))

require 'mongo'
require 'benchmark'
require_relative 'benchmark_helper'

##
# Perform 'lightweight' benchmarks. This includes:
#
#   Run command
#   Find one by ID
#   Small doc insertOne
#   Large doc insertOne
#
##
def lightweight_benchmark!

  #bench_helper = BenchmarkHelper.new('perftest','corpus')
  bench_helper = BenchmarkHelper.new('foo','bar')
  database = bench_helper.database
  collection = bench_helper.collection
  print "\n\n\n"



  ##
  # Run command
  #
  # Measure: run isMaster command 10,000 times
  ##
  first = Benchmark.bmbm do |bm|
    bm.report('Lightweight::Run Command') do
      10000.times do |i|
        database.command(:ismaster => 1)
      end
    end
  end
  print "\n\n\n"



  ##
  # Find one by ID
  #
  # - Drop 'perftest' database
  # - Load TWITTER dataset
  # - Insert the first 10,000 documents individually into the 'corpus' collection, adding
  #   sequential _id fields to each before upload
  #
  # Measure: for each of the 10,000 sequential _id numbers, issue a find command
  #          for that _id on the 'corpus' collection and retrieve the single-document result.
  #
  # - Drop 'perftest' database
  ##
  database.drop
  twitter_data = BenchmarkHelper.load_array_from_file('TWITTER.txt')
  twitter_data_size = twitter_data.size

  5.times do |i|
    next unless (i < twitter_data_size)
    twitter_data[i][:_id] = i
    collection.insert_one( twitter_data[i] )
  end

  second = Benchmark.bmbm do |bm|
    bm.report('Lightweight::Find one by ID') do
      5.times do |i|
        collection.find(:_id => i).first
      end
    end
  end

  database.drop
  print "\n\n\n"



  ##
  # Small doc insertOne
  #
  # - Drop 'perftest' database
  # - Load SMALL_DOC dataset
  # - Drop the 'corpus' collection.
  #
  # Measure: insert the first 10,000 documents individually into the 'corpus' collection
  #          using insert_one. DO NOT manually add an _id field.
  #
  # - Drop 'perftest' database
  ##
  database.drop
  small_doc_data = BenchmarkHelper.load_array_from_file('SMALL_DOC.txt')
  small_doc_data_size = small_doc_data.size
  collection.drop

  third = Benchmark.bmbm do |bm|
    bm.report('Lightweight::Small doc insertOne') do
      5.times do |i|
        collection.insert_one( small_doc_data[i] ) if (i < small_doc_data_size)
      end
    end
  end

  database.drop
  print "\n\n\n"



  ##
  # Large doc insertOne
  #
  # - Drop 'perftest' database
  # - Load LARGE_DOC dataset
  # - Drop the 'corpus' collection.
  #
  # Measure: insert the first 1,000 documents individually into the 'corpus' collection
  #          using insert_one. DO NOT manually add an _id field.
  #
  # - Drop 'perftest' database
  ##
  database.drop
  small_doc_data = BenchmarkHelper.load_array_from_file('LARGE_DOC.txt')
  small_doc_data_size = small_doc_data.size
  collection.drop

  fourth = Benchmark.bmbm do |bm|
    bm.report('Lightweight::Large doc insertOne') do
      5.times do |i|
        collection.insert_one( small_doc_data[i] ) if (i < small_doc_data_size)
      end
    end
  end

  database.drop
  print "\n\n\n"

end
