require 'lite_spec_helper'

require 'runners/connection_string'

describe 'Uri Options' do
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

          context 'when the uri should not warn', if: !test.warn? && test.valid? do

            before do
              expect(Mongo::Logger.logger).not_to receive(:warn)
            end

            it 'does not raise an exception or warning' do
              expect(test.client).to be_a(Mongo::Client)
            end
          end

          context 'when the uri is valid', if: test.valid? do

            if test.hosts
              it 'creates a client with the correct hosts' do
                expect(test.client).to have_hosts(test, test.hosts)
              end
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
