require 'lite_spec_helper'

describe 'Uri Options' do
  include Mongo::ConnectionString

  URI_OPTIONS_TESTS.each do |file|

    spec = Mongo::ConnectionString::Spec.new(file)

    context(spec.description) do

      before(:all) do

        module Mongo
          class Address

            private

            alias :original_create_resolver :create_resolver
            def create_resolver(timeout, ssl_options)
              family = (host == 'localhost') ? ::Socket::AF_INET : ::Socket::AF_UNSPEC
              info = ::Socket.getaddrinfo(host, nil, family, ::Socket::SOCK_STREAM)
              FAMILY_MAP[info.first[4]].new(info[3], port, host)
            end
          end
        end
      end

      after(:all) do

        module Mongo
          # Return the implementations to their originals for the other
          # tests in the suite.
          class Address
            alias :create_resolver :original_create_resolver
            remove_method(:original_create_resolver)
          end
        end
      end

      spec.tests.each do |test|

        # Skip optional tests for options the driver doesn't support.
        next if test.description.start_with?('tlsAllowInvalidHostnames')

        context "#{test.description}" do

          context 'when the uri should warn', if: test.warn? do

            before do
              expect(Mongo::Logger.logger).to receive(:warn)
            end

            it 'warns' do
              expect(test.client).to be_a(Mongo::Client)
            end
          end

          context 'when the uri is invalid', unless: test.valid? do

            it 'raises an error' do
              expect{
                test.uri
              }.to raise_exception(Mongo::Error::InvalidURI)
            end
          end

          context 'when the uri should not warn', if: !test.warn? do

            before do
              expect(Mongo::Logger.logger).not_to receive(:warn)
            end

            it 'does not raise an exception or warning' do
              expect(test.client).to be_a(Mongo::Client)
            end
          end

          context 'when the uri is valid', if: test.valid? do

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
