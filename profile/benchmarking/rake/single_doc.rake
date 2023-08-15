# frozen_string_literal: true

namespace :single_doc do
  task :command do
    puts 'SINGLE DOC BENCHMARK :: COMMAND'
    Mongo::Benchmarking::SingleDoc.run(:command)
  end

  # Requirement: A file in Mongo::Benchmarking::DATA_PATH, called TWEET.json.
  task :find_one do
    puts 'SINGLE DOC BENCHMARK :: FIND ONE BY ID'
    Mongo::Benchmarking::SingleDoc.run(:find_one)
  end

  # Requirement: A file in Mongo::Benchmarking::DATA_PATH, called SMALL_DOC.json.
  task :insert_one_small do
    puts 'SINGLE DOC BENCHMARK :: INSERT ONE SMALL DOCUMENT'
    Mongo::Benchmarking::SingleDoc.run(:insert_one_small)
  end

  # Requirement: A file in Mongo::Benchmarking::DATA_PATH, called LARGE_DOC.json.
  task :insert_one_large do
    puts 'SINGLE DOC BENCHMARK :: INSERT ONE LARGE DOCUMENT'
    Mongo::Benchmarking::SingleDoc.run(:insert_one_large)
  end
end
