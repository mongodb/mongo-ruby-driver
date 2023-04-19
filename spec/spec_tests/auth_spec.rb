# frozen_string_literal: true
# rubocop:todo all

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

          if test.valid?

            context 'the auth configuration is valid' do
              if test.credential

                it 'creates a client with options matching the credential' do
                  expect(test.actual_client_options).to eq(test.expected_credential)
                end

                it 'creates a user with attributes matching the credential' do
                  expect(test.actual_user_attributes).to eq(test.expected_credential)
                end
              else

                context 'with empty credentials' do
                  it 'creates a client with no credential information' do
                    expect(test.client).to have_blank_credentials
                  end
                end
              end
            end

          else

            context 'the auth configuration is invalid' do
              it 'raises an error' do
                expect do
                  test.client
                end.to raise_error(Mongo::Auth::InvalidConfiguration)
              end
            end

          end
        end
      end
    end
  end
end
