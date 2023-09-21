# frozen_string_literal: true
# rubocop:todo all

namespace :multi_doc do
  # Requirement: A file in Mongo::Benchmarking::DATA_PATH, called TWEET.json.
  task :find_many do
    puts 'MULTI DOCUMENT BENCHMARK :: FIND MANY'
    Mongo::Benchmarking::MultiDoc.run(:find_many)
  end

  # Requirement: A file in Mongo::Benchmarking::DATA_PATH, called SMALL_DOC.json.
  task :bulk_insert_small do
    puts 'MULTI DOCUMENT BENCHMARK :: BULK INSERT SMALL'
    Mongo::Benchmarking::MultiDoc.run(:bulk_insert_small)
  end

  # Requirement: A file in Mongo::Benchmarking::DATA_PATH, called LARGE_DOC.json.
  task :bulk_insert_large do
    puts 'MULTI DOCUMENT BENCHMARK :: BULK INSERT LARGE'
    Mongo::Benchmarking::MultiDoc.run(:bulk_insert_large)
  end

  # Requirement: A file in Mongo::Benchmarking::DATA_PATH, called GRIDFS_LARGE.
  task :gridfs_upload do
    puts 'MULTI DOCUMENT BENCHMARK :: GRIDFS UPLOAD'
    Mongo::Benchmarking::MultiDoc.run(:gridfs_upload)
  end

  # Requirement: A file in Mongo::Benchmarking::DATA_PATH, called GRIDFS_LARGE.
  task :gridfs_download do
    puts 'MULTI DOCUMENT BENCHMARK :: GRIDFS DOWNLOAD'
    Mongo::Benchmarking::MultiDoc.run(:gridfs_download)
  end
end
