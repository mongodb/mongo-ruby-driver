# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

require 'runners/connection_string'

describe 'URI options' do
  include Mongo::ConnectionString

  # Since the tests issue global assertions on Mongo::Logger,
  # we need to close all clients/stop monitoring to avoid monitoring
  # threads warning and interfering with these assertions
  clean_slate_for_all_if_possible

  URI_OPTIONS_TESTS.each do |file|

    spec = Mongo::ConnectionString::Spec.new(file)

    context(spec.description) do

      spec.tests.each do |test|
        context "#{test.description}" do
          if test.description.downcase.include?("gssapi")
            require_mongo_kerberos
          end

          if test.valid?

            # The warning assertion needs to be first because the test caches
            # the client instance, and subsequent examples don't instantiate it
            # again.
            if test.warn?
              it 'warns' do
                expect(Mongo::Logger.logger).to receive(:warn)#.and_call_original
                expect(test.client).to be_a(Mongo::Client)
              end
            else
              it 'does not warn' do
                expect(Mongo::Logger.logger).not_to receive(:warn)
                expect(test.client).to be_a(Mongo::Client)
              end
            end

            if test.hosts
              it 'creates a client with the correct hosts' do
                expect(test.client).to have_hosts(test, test.hosts)
              end
            end

            it 'creates a client with the correct authentication properties' do
              expect(test.client).to match_auth(test)
            end

            if opts = test.expected_options
              if opts['compressors'] && opts['compressors'].include?('snappy')
                before do
                  unless ENV.fetch('BUNDLE_GEMFILE', '') =~ /snappy/
                    skip "This test requires snappy compression"
                  end
                end
              end

              if opts['compressors'] && opts['compressors'].include?('zstd')
                before do
                  unless ENV.fetch('BUNDLE_GEMFILE', '') =~ /zstd/
                    skip "This test requires zstd compression"
                  end
                end
              end

              it 'creates a client with the correct options' do
                mapped = Mongo::URI::OptionsMapper.new.ruby_to_smc(test.client.options)
                expected = Mongo::ConnectionString.adjust_expected_mongo_client_options(
                  opts,
                )
                mapped.should == expected
              end
            end

          else

            it 'raises an error' do
              expect{
                test.uri
              }.to raise_exception(Mongo::Error::InvalidURI)
            end
          end
        end
      end
    end
  end
end
