# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

require 'support/using_hash'
require 'runners/connection_string'
require 'mrss/lite_constraints'

SEED_LIST_DISCOVERY_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/seed_list_discovery/**/*.yml").sort

describe 'DNS Seedlist Discovery' do
  require_external_connectivity

  include Mongo::ConnectionString

  SEED_LIST_DISCOVERY_TESTS.each do |test_path|

    spec = ::Utils.load_spec_yaml_file(test_path)

    test = Mongo::ConnectionString::Test.new(spec)

    context(File.basename(test_path)) do

      if test.raise_error?
        context 'the uri is invalid' do
          retry_test

          let(:valid_errors) do
            [
              Mongo::Error::InvalidTXTRecord,
              Mongo::Error::NoSRVRecords,
              Mongo::Error::InvalidURI,
              Mongo::Error::MismatchedDomain,
              # This is unfortunate. RUBY-2624
              ArgumentError,
            ]
          end

          let(:error) do
            begin
              test.client
            rescue => ex
            end
            ex
          end

          # In Evergreen sometimes this test fails intermittently.
          it 'raises an error' do
            expect(valid_errors).to include(error.class)
          end
        end

      else

        context 'the uri is valid' do
          retry_test
          # In Evergreen sometimes this test fails intermittently.
          it 'does not raise an exception' do
            expect(test.uri).to be_a(Mongo::URI::SRVProtocol)
          end

          if test.seeds
            # DNS seed list tests specify both seeds and hosts.
            # To get the hosts, the client must do SDAM (as required in the
            # spec tests' description), but this isn't testing DNS seed list -
            # it is testing SDAM. Plus, all of the hosts are always the same.
            # If seed list is given in the expectations, just test the seed
            # list and not the expanded hosts.
            it 'creates a client with the correct seeds' do
              expect(test.client).to have_hosts(test, test.seeds)
            end
          elsif test.num_seeds
            it 'has the right number of seeds' do
              num_servers = test.client.cluster.servers_list.length
              expect(num_servers).to eq(test.num_seeds)
            end
          else
            it 'creates a client with the correct hosts' do
              expect(test.client).to have_hosts(test, test.hosts)
            end
          end

          if test.expected_options
            it 'creates a client with the correct uri options' do
              mapped = Mongo::URI::OptionsMapper.new.ruby_to_smc(test.client.options)
              # Connection string spec tests do not use canonical URI option names
              actual = Utils.downcase_keys(mapped)
              expected = Utils.downcase_keys(test.expected_options)
              # SRV tests use ssl URI option instead of tls one
              if expected.key?('ssl') && !expected.key?('tls')
                expected['tls'] = expected.delete('ssl')
              end
              # The client object contains auth source in options which
              # isn't asserted in some tests.
              if actual.key?('authsource') && !expected.key?('authsource')
                actual.delete('authsource')
              end
              actual.should == expected
            end
          end

          if test.non_uri_options
            it 'creates a client with the correct non-uri options' do
              opts = UsingHash[test.non_uri_options]
              if user = opts.use('user')
                test.client.options[:user].should == user
              end
              if password = opts.use('password')
                test.client.options[:password].should == password
              end
              if db = opts.use('db')
                test.client.database.name.should == db
              end
              if auth_source = opts.use('auth_database')
                Mongo::Auth::User.new(test.client.options).auth_source == auth_source
              end
              unless opts.empty?
                raise "Unhandled keys: #{opts}"
              end
            end
          end
        end
      end
    end
  end
end
