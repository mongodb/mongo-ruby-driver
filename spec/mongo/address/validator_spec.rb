# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

class ValidatorHost
  include Mongo::Address::Validator
end

describe Mongo::Address::Validator do
  let(:host) { ValidatorHost.new }

  describe '#validate_address_str!' do
    shared_examples_for 'raises InvalidAddress' do
      it 'raises InvalidAddress' do
        expect do
          host.validate_address_str!(address_str)
        end.to raise_error(Mongo::Error::InvalidAddress)
      end
    end

    shared_examples_for 'passes validation' do
      it 'passes validation' do
        expect do
          host.validate_address_str!(address_str)
        end.not_to raise_error
      end
    end

    context 'leading dots' do
      let(:address_str) { '.foo.bar.com' }

      it_behaves_like 'raises InvalidAddress'
    end

    context 'trailing dots' do
      let(:address_str) { 'foo.bar.com.' }

      it_behaves_like 'raises InvalidAddress'
    end

    context 'runs of multiple dots' do
      let(:address_str) { 'foo..bar.com' }

      it_behaves_like 'raises InvalidAddress'
    end

    context 'no dots' do
      let(:address_str) { 'foo' }

      it_behaves_like 'passes validation'
    end
  end
end
