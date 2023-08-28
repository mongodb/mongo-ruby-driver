# frozen_string_literal: true
# rubocop:todo all

namespace :parallel do
  # Requirement: A directory in Mongo::Benchmarking::DATA_PATH, called LDJSON_MULTI,
  # with the files used in this task.
  task :import do
    puts 'PARALLEL ETL BENCHMARK :: IMPORT'
    Mongo::Benchmarking::Parallel.run(:import)
  end

  # Requirement: A directory in Mongo::Benchmarking::DATA_PATH, called LDJSON_MULTI,
  # with the files used in this task.
  # Requirement: Another directory in '#{Mongo::Benchmarking::DATA_PATH}/LDJSON_MULTI'
  # called 'output'.
  task :export do
    puts 'PARALLEL ETL BENCHMARK :: EXPORT'
    Mongo::Benchmarking::Parallel.run(:export)
  end

  # Requirement: A directory in Mongo::Benchmarking::DATA_PATH, called GRIDFS_MULTI,
  # with the files used in this task.
  task :gridfs_upload do
    puts 'PARALLEL ETL BENCHMARK :: GRIDFS UPLOAD'
    Mongo::Benchmarking::Parallel.run(:gridfs_upload)
  end

  # Requirement: A directory in Mongo::Benchmarking::DATA_PATH, called GRIDFS_MULTI,
  # with the files used in this task.
  # Requirement: Another directory in '#{Mongo::Benchmarking::DATA_PATH}/GRIDFS_MULTI'
  # called 'output'.
  task :gridfs_download do
    puts 'PARALLEL ETL BENCHMARK :: GRIDFS DOWNLOAD'
    Mongo::Benchmarking::Parallel.run(:gridfs_download)
  end
end
