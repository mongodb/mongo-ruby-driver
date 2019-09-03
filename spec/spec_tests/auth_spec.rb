require 'spec_helper'

describe 'Auth' do
  include Mongo::Auth

  clean_slate_for_all

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
              expect {
                test.client
            }.to raise_error(Mongo::Auth::InvalidConfiguration)
            end
          end

          context 'when the auth configuration is valid', if: test.valid? do
            it 'creates a client with the correct credentials' do
              expect(test.client).to match_credential(test)
            end
          end
        end
      end
    end
  end
end
