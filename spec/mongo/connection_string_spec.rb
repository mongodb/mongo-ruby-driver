require 'spec_helper'

describe 'ConnectionString' do
  include Mongo::ConnectionString

  CONNECTION_STRING_TESTS.each do |file|

    spec = Mongo::ConnectionString::Spec.new(file)

    context(spec.description) do

      before(:all) do

        module Mongo
          class Address

            private

            alias :original_initialize_resolver! :initialize_resolver!
            def initialize_resolver!(timeout, ssl_options)
              family = (host == 'localhost') ? ::Socket::AF_INET : ::Socket::AF_UNSPEC
              info = ::Socket.getaddrinfo(host, nil, family, ::Socket::SOCK_STREAM)
              FAMILY_MAP[info.first[4]].new(info[3], port, host)
            end
          end

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
            def disconnect!; true; end
          end
        end
      end

      after(:all) do

        module Mongo
          # Return the implementations to their originals for the other
          # tests in the suite.
          class Address
            alias :initialize_resolver! :original_initialize_resolver!
            remove_method(:original_initialize_resolver!)
          end

          class Server
            alias :initialize :original_initialize
            remove_method(:original_initialize)

            alias :disconnect! :original_disconnect!
            remove_method(:original_disconnect!)
          end
        end
      end

      spec.tests.each_with_index do |test, index|

        context "when a #{test.description} is provided" do


          context 'when the uri is invalid', unless: test.valid? do

            it 'raises an error' do
              expect{
                test.uri
              }.to raise_exception(Mongo::Error::InvalidURI)
            end
          end

          context 'when the uri should warn', if: test.warn? do

            before do
              expect(Mongo::Logger.logger).to receive(:warn)
            end

            it 'warns' do
              expect(test.client).to be_a(Mongo::Client)
            end
          end

          context 'when the uri is valid', if: test.valid? do

            it 'does not raise an exception' do
              expect(test.uri).to be_a(Mongo::URI)
            end

            it 'creates a client with the correct hosts' do
              expect(test.client).to have_hosts(test)
            end

            it 'creates a client with the correct authentication properties' do
              expect(test.client).to match_auth(test)
            end

            it 'creates a client with the correct options' do
              expect(test.client).to match_options(test)
            end
          end
        end
      end
    end
  end
end
