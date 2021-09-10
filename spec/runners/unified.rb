# frozen_string_literal: true
# encoding: utf-8

require 'support/using_hash'
require 'runners/unified/error'
require 'runners/unified/entity_map'
require 'runners/unified/event_subscriber'
require 'runners/unified/test'
require 'runners/unified/test_group'

def define_unified_spec_tests(base_path, paths, expect_failure: false)
  paths.each do |path|
    basename = path[base_path.length+1...path.length]
    context basename do
      group = Unified::TestGroup.new(path)

      if basename =~ /retryable|transaction/
        require_wired_tiger
      end

      group.tests.each do |test|
        context test.description do

          if test.skip?
            before do
              skip test.skip_reason
            end
          end

          before(:all) do
            if SpecConfig.instance.retry_reads == false
              skip "Tests are not applicable when legacy read retries are used"
            end
            if SpecConfig.instance.retry_writes == false
              skip "Tests are not applicable when legacy write retries are used"
            end

            if ClusterConfig.instance.topology == :sharded
              if test.require_multiple_mongoses? && SpecConfig.instance.addresses.length == 1
                skip "Test requires multiple mongoses"
              elsif test.require_single_mongos? && SpecConfig.instance.addresses.length > 1
                # Many transaction spec tests that do not specifically deal with
                # sharded transactions fail when run against a multi-mongos cluster
                skip "Test requires single mongos"
              end
            end
          end

          if expect_failure
            it 'fails as expected' do
              if test.group_reqs
                unless test.group_reqs.any? { |r| r.satisfied? }
                  skip "Group requirements not satisfied"
                end
              end
              if test.reqs
                unless test.reqs.any? { |r| r.satisfied? }
                  skip "Requirements not satisfied"
                end
              end
              begin
                test.create_entities
                test.set_initial_data
                lambda do
                  test.run
                  test.assert_outcome
                  test.assert_events
                # HACK: other errors are possible and likely will need to
                # be added here later as the tests evolve.
                end.should raise_error(Mongo::Error::OperationFailure)
              ensure
                test.cleanup
              end
            end
          else
            it 'passes' do
              if test.group_reqs
                unless test.group_reqs.any? { |r| r.satisfied? }
                  skip "Group requirements not satisfied"
                end
              end
              if test.reqs
                unless test.reqs.any? { |r| r.satisfied? }
                  skip "Requirements not satisfied"
                end
              end
              test.create_entities
              test.set_initial_data
              test.run
              test.assert_outcome
              test.assert_events
              test.cleanup
            end
          end
        end
      end
    end
  end
end
