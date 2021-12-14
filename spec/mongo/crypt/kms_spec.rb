# frozen_string_literal: true
# encoding: utf-8

require 'mongo'
require 'lite_spec_helper'

describe Mongo::Crypt::KMS do
  context 'Validations' do
    context '.validate_tls_options' do
      it 'returns valid options for nil parameter' do
        expect(
          Mongo::Crypt::KMS::Validations.validate_tls_options(nil)
        ).to eq({})
      end

      it 'accepts empty hash' do
        expect(
          Mongo::Crypt::KMS::Validations.validate_tls_options({})
        ).to eq({})
      end

      it 'does not allow disabled ssl' do
        expect {
          Mongo::Crypt::KMS::Validations.validate_tls_options(
            {
              aws: {ssl: false}
            }
          )
        }.to raise_error(ArgumentError, /TLS is required/)
      end

      it 'does not allow insecure tls options' do
        %i(
          ssl_verify_certificate
          ssl_verify_hostname
          ssl_verify_ocsp_endpoint
        ).each do |insecure_opt|
          expect {
            Mongo::Crypt::KMS::Validations.validate_tls_options(
              {
                aws: {insecure_opt => false}
              }
            )
          }.to raise_error(ArgumentError, /Insecure TLS options prohibited/)
        end
      end

      it 'allows valid options' do
        options = {
          aws: {
            ssl: true,
            ssl_cert_string: 'Content is not validated',

          }
        }
      end
    end
  end
end