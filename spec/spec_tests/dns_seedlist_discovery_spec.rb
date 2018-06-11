require 'spec_helper'

describe 'DNS Seedlist Discovery' do

  if test_connecting_externally?

    include Mongo::ConnectionString

    before(:all) do

      module Mongo
        class Server
          # The constructor keeps the same API, but does not instantiate a
          # monitor and run it.
          alias :original_initialize :initialize

          def initialize(address, cluster, monitoring, event_listeners, options = {})
            @address = address
            @cluster = cluster
            @monitoring = monitoring
            @options = options.freeze
            @monitor = Monitor.new(address, event_listeners, options)
          end

          # Disconnect simply needs to return true since we have no monitor and
          # no connection.
          alias :original_disconnect! :disconnect!

          def disconnect!;
            true;
          end
        end
      end
    end

    after(:all) do

      module Mongo
        class Server
          alias :initialize :original_initialize
          remove_method(:original_initialize)

          alias :disconnect! :original_disconnect!
          remove_method(:original_disconnect!)
        end
      end
    end

    DNS_SEEDLIST_DISCOVERY_TESTS.each do |file_name|

      file = File.new(file_name)
      spec = YAML.load(ERB.new(file.read).result)
      file.close

      test = Mongo::ConnectionString::Test.new(spec)

      context(File.basename(file_name)) do

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

          it 'creates a client with the correct hosts' do
            expect(test.client).to have_hosts(test)
          end

          it 'creates a client with the correct options' do
            expect(test.client).to match_options(test)
          end
        end
      end
    end
  end
end
