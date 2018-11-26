require 'spec_helper'

describe 'DNS Seedlist Discovery' do
  require_external_connectivity

  include Mongo::ConnectionString

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
