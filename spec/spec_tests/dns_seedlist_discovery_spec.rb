# frozen_string_literal: true
# encoding: utf-8

require 'lite_spec_helper'

require 'runners/connection_string'

describe 'DNS Seedlist Discovery' do
  require_external_connectivity

  include Mongo::ConnectionString

  DNS_SEEDLIST_DISCOVERY_TESTS.each do |test_path|

    spec = YAML.load(File.read(test_path))

    test = Mongo::ConnectionString::Test.new(spec)

    context(File.basename(test_path)) do

      context 'when the uri is invalid', if: test.raise_error? do

        let(:valid_errors) do
          [
            Mongo::Error::InvalidTXTRecord,
            Mongo::Error::NoSRVRecords,
            Mongo::Error::InvalidURI,
            Mongo::Error::MismatchedDomain,
          ]
        end

        let(:error) do
          e = nil
          begin; test.uri; rescue => ex; e = ex; end
          e
        end

        it 'raises an error' do
          expect(valid_errors).to include(error.class)
        end
      end

      context 'when the uri is valid', unless: test.raise_error? do

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
        else
          it 'creates a client with the correct hosts' do
            expect(test.client).to have_hosts(test, test.hosts)
          end
        end

        it 'creates a client with the correct options' do
          mapped = Mongo::URI::OptionsMapper.new.ruby_to_smc(test.client.options)
          # Connection string spec tests do not use canonical URI option names
          actual = Utils.downcase_keys(mapped)
          expected = Utils.downcase_keys(test.options)
          # SRV tests use ssl URI option instead of tls one
          if expected.key?('ssl') && !expected.key?('tls')
            expected['tls'] = expected.delete('ssl')
          end
          actual.should == expected
        end
      end
    end
  end
end
