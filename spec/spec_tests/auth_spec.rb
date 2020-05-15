require 'lite_spec_helper'

require 'runners/auth'

describe 'Auth' do
  include Mongo::Auth

  AUTH_TESTS.each do |file|
    spec = Mongo::Auth::Spec.new(file)

    context(spec.description) do
      spec.tests.each_with_index do |test, index|
        context test.description do
          if test.description.downcase.include?("gssapi")
            require_mongo_kerberos
          end

          context 'when the auth configuration is invalid', unless: test.valid? do
            it 'raises an error' do
              expect do
                test.client
              end.to raise_error(Mongo::Auth::InvalidConfiguration)
            end
          end

          context 'when the auth configuration is valid' do
            context 'with empty credentials', if: test.valid? && test.credential.nil? do
              it 'creates a client with no credential information' do
                expect(test.client).to have_blank_credentials
              end
            end

            it 'creates a client with the correct credentials', if: test.valid? && test.credential do
              expect(test.received_credential).to eq(test.expected_credential)
            end
          end
        end
      end
    end
  end
end
