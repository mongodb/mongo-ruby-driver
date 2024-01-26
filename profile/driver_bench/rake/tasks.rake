# frozen_string_literal: true

require_relative '../suite'

task driver_bench: %i[ driver_bench:data driver_bench:run ]

SPECS_REPO_URI = 'git@github.com:mongodb/specifications.git'
SPECS_PATH = File.expand_path('../../../specifications', __dir__)
DRIVER_BENCH_DATA = File.expand_path('../../data/driver_bench', __dir__)

namespace :driver_bench do
  desc 'Downloads the DriverBench data files, if necessary'
  task :data do
    if File.directory?('./profile/data/driver_bench')
      puts 'DriverBench data files are already downloaded'
      next
    end

    if File.directory?(SPECS_PATH)
      puts 'specifications repo is already checked out'
    else
      sh 'git', 'clone', SPECS_REPO_URI
    end

    mkdir_p DRIVER_BENCH_DATA

    Dir.glob(File.join(SPECS_PATH, 'source/benchmarking/data/*.tgz')) do |archive|
      Dir.chdir(DRIVER_BENCH_DATA) do
        sh 'tar', 'xzf', archive
      end
    end
  end

  desc 'Runs the DriverBench benchmark suite'
  task :run do
    Mongo::DriverBench::Suite.run!
  end
end
