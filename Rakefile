# frozen_string_literal: true
# rubocop:todo all

require 'bundler'
require 'bundler/gem_tasks'
require 'rspec/core/rake_task'
# TODO move the mongo require into the individual tasks that actually need it
require 'mongo'

ROOT = File.expand_path(File.join(File.dirname(__FILE__)))

$: << File.join(ROOT, 'spec/shared/lib')

require 'mrss/spec_organizer'

CLASSIFIERS = [
  [%r,^mongo/server,, :unit_server],
  [%r,^mongo,, :unit],
  [%r,^kerberos,, :unit],
  [%r,^integration/sdam_error_handling,, :sdam_integration],
  [%r,^integration/cursor_reaping,, :cursor_reaping],
  [%r,^integration/query_cache,, :query_cache],
  [%r,^integration/transactions_examples,, :tx_examples],
  [%r,^(atlas|integration),, :integration],
  [%r,^spec_tests/sdam_integration,, :spec_sdam_integration],
  [%r,^spec_tests,, :spec],
]

RUN_PRIORITY = %i(
  tx_examples
  unit unit_server
  integration sdam_integration cursor_reaping query_cache
  spec spec_sdam_integration
)

tasks = Rake.application.instance_variable_get('@tasks')
tasks['release:do'] = tasks.delete('release')

RSpec::Core::RakeTask.new(:spec) do |t|
  #t.rspec_opts = "--profile 5" if ENV['CI']
end

task :default => ['spec:prepare', :spec]

namespace :spec do
  desc 'Creates necessary user accounts in the cluster'
  task :prepare do
    $: << File.join(File.dirname(__FILE__), 'spec')

    require 'support/utils'
    require 'support/spec_setup'
    SpecSetup.new.run
  end

  desc 'Waits for sessions to be available in the deployment'
  task :wait_for_sessions do
    $: << File.join(File.dirname(__FILE__), 'spec')

    require 'support/utils'
    require 'support/spec_config'
    require 'support/client_registry'

    client = ClientRegistry.instance.global_client('authorized')
    client.database.command(ping: 1)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 300
    loop do
      begin
        client.cluster.validate_session_support!
        break
      rescue Mongo::Error::SessionsNotSupported
        if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
          raise "Sessions did not become supported in 300 seconds"
        end
        client.cluster.scan!
      end
    end
  end

  desc 'Prints configuration used by the test suite'
  task :config do
    $: << File.join(File.dirname(__FILE__), 'spec')

    # Since this task is usually used for troubleshooting of test suite
    # configuration, leave driver log level at the default of debug to
    # have connection diagnostics printed during handshakes and such.
    require 'support/utils'
    require 'support/spec_config'
    require 'support/client_registry'
    SpecConfig.instance.print_summary
  end

  def spec_organizer
    Mrss::SpecOrganizer.new(
      root: ROOT,
      classifiers: CLASSIFIERS,
      priority_order: RUN_PRIORITY,
    )
  end

  task :ci => ['spec:prepare'] do
    spec_organizer.run
  end

  desc 'Show test buckets'
  task :buckets do
    spec_organizer.ordered_buckets.each do |category, paths|
      puts "#{category || 'remaining'}: #{paths&.join(' ') || '<none>'}"
    end
  end
end

namespace :release do
  task :check_private_key do
    unless File.exist?('gem-private_key.pem')
      raise "No private key present, cannot release"
    end
  end
end

task :release => ['release:check_private_key', 'release:do']

desc "Generate all documentation"
task :docs => 'docs:yard'

namespace :docs do
  desc "Generate yard documention"
  task :yard do
    out = File.join('yard-docs', Mongo::VERSION)
    FileUtils.rm_rf(out)
    system "yardoc -o #{out} --title mongo-#{Mongo::VERSION}"
  end
end

require_relative "profile/benchmarking"

# Some require data files, available from the drivers team. See the comments above each task for details."
namespace :benchmark do
  desc "Run the driver benchmark tests."

  namespace :micro do
    desc "Run the common driver micro benchmarking tests"

    namespace :flat do
      desc "Benchmarking for flat bson documents."

      # Requirement: A file in Mongo::Benchmarking::DATA_PATH, called flat_bson.json.
      task :encode do
        puts "MICRO BENCHMARK:: FLAT:: ENCODE"
        Mongo::Benchmarking::Micro.run(:flat, :encode)
      end

      # Requirement: A file in Mongo::Benchmarking::DATA_PATH, called flat_bson.json.
      task :decode do
        puts "MICRO BENCHMARK:: FLAT:: DECODE"
        Mongo::Benchmarking::Micro.run(:flat, :decode)
      end
    end

    namespace :deep do
      desc "Benchmarking for deep bson documents."

      # Requirement: A file in Mongo::Benchmarking::DATA_PATH, called deep_bson.json.
      task :encode do
        puts "MICRO BENCHMARK:: DEEP:: ENCODE"
        Mongo::Benchmarking::Micro.run(:deep, :encode)
      end

      # Requirement: A file in Mongo::Benchmarking::DATA_PATH, called deep_bson.json.
      task :decode do
        puts "MICRO BENCHMARK:: DEEP:: DECODE"
        Mongo::Benchmarking::Micro.run(:deep, :decode)
      end
    end

    namespace :full do
      desc "Benchmarking for full bson documents."

      # Requirement: A file in Mongo::Benchmarking::DATA_PATH, called full_bson.json.
      task :encode do
        puts "MICRO BENCHMARK:: FULL:: ENCODE"
        Mongo::Benchmarking::Micro.run(:full, :encode)
      end

      # Requirement: A file in Mongo::Benchmarking::DATA_PATH, called full_bson.json.
      task :decode do
        puts "MICRO BENCHMARK:: FULL:: DECODE"
        Mongo::Benchmarking::Micro.run(:full, :decode)
      end
    end
  end

  namespace :single_doc do
    desc "Run the common driver single-document benchmarking tests"
    task :command do
      puts "SINGLE DOC BENCHMARK:: COMMAND"
      Mongo::Benchmarking::SingleDoc.run(:command)
    end

    # Requirement: A file in Mongo::Benchmarking::DATA_PATH, called TWEET.json.
    task :find_one do
      puts "SINGLE DOC BENCHMARK:: FIND ONE BY ID"
      Mongo::Benchmarking::SingleDoc.run(:find_one)
    end

    # Requirement: A file in Mongo::Benchmarking::DATA_PATH, called SMALL_DOC.json.
    task :insert_one_small do
      puts "SINGLE DOC BENCHMARK:: INSERT ONE SMALL DOCUMENT"
      Mongo::Benchmarking::SingleDoc.run(:insert_one_small)
    end

    # Requirement: A file in Mongo::Benchmarking::DATA_PATH, called LARGE_DOC.json.
    task :insert_one_large do
      puts "SINGLE DOC BENCHMARK:: INSERT ONE LARGE DOCUMENT"
      Mongo::Benchmarking::SingleDoc.run(:insert_one_large)
    end
  end

  namespace :multi_doc do
    desc "Run the common driver multi-document benchmarking tests"

    # Requirement: A file in Mongo::Benchmarking::DATA_PATH, called TWEET.json.
    task :find_many do
      puts "MULTI DOCUMENT BENCHMARK:: FIND MANY"
      Mongo::Benchmarking::MultiDoc.run(:find_many)
    end

    # Requirement: A file in Mongo::Benchmarking::DATA_PATH, called SMALL_DOC.json.
    task :bulk_insert_small do
      puts "MULTI DOCUMENT BENCHMARK:: BULK INSERT SMALL"
      Mongo::Benchmarking::MultiDoc.run(:bulk_insert_small)
    end

    # Requirement: A file in Mongo::Benchmarking::DATA_PATH, called LARGE_DOC.json.
    task :bulk_insert_large do
      puts "MULTI DOCUMENT BENCHMARK:: BULK INSERT LARGE"
      Mongo::Benchmarking::MultiDoc.run(:bulk_insert_large)
    end

    # Requirement: A file in Mongo::Benchmarking::DATA_PATH, called GRIDFS_LARGE.
    task :gridfs_upload do
      puts "MULTI DOCUMENT BENCHMARK:: GRIDFS UPLOAD"
      Mongo::Benchmarking::MultiDoc.run(:gridfs_upload)
    end

    # Requirement: A file in Mongo::Benchmarking::DATA_PATH, called GRIDFS_LARGE.
    task :gridfs_download do
      puts "MULTI DOCUMENT BENCHMARK:: GRIDFS DOWNLOAD"
      Mongo::Benchmarking::MultiDoc.run(:gridfs_download)
    end
  end

  namespace :parallel do
    desc "Run the common driver paralell ETL benchmarking tests"

    # Requirement: A directory in Mongo::Benchmarking::DATA_PATH, called LDJSON_MULTI,
    # with the files used in this task.
    task :import do
      puts "PARALLEL ETL BENCHMARK:: IMPORT"
      Mongo::Benchmarking::Parallel.run(:import)
    end

    # Requirement: A directory in Mongo::Benchmarking::DATA_PATH, called LDJSON_MULTI,
    # with the files used in this task.
    # Requirement: Another directory in "#{Mongo::Benchmarking::DATA_PATH}/LDJSON_MULTI"
    # called 'output'.
    task :export do
      puts "PARALLEL ETL BENCHMARK:: EXPORT"
      Mongo::Benchmarking::Parallel.run(:export)
    end

    # Requirement: A directory in Mongo::Benchmarking::DATA_PATH, called GRIDFS_MULTI,
    # with the files used in this task.
    task :gridfs_upload do
      puts "PARALLEL ETL BENCHMARK:: GRIDFS UPLOAD"
      Mongo::Benchmarking::Parallel.run(:gridfs_upload)
    end

    # Requirement: A directory in Mongo::Benchmarking::DATA_PATH, called GRIDFS_MULTI,
    # with the files used in this task.
    # Requirement: Another directory in "#{Mongo::Benchmarking::DATA_PATH}/GRIDFS_MULTI"
    # called 'output'.
    task :gridfs_download do
      puts "PARALLEL ETL BENCHMARK:: GRIDFS DOWNLOAD"
      Mongo::Benchmarking::Parallel.run(:gridfs_download)
    end
  end
end
