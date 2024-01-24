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

load 'profile/benchmarking/rake/tasks.rake'
