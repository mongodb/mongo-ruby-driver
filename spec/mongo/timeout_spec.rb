# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::Timeout do
  describe '#timeout' do
    let(:default_error_message) { 'execution expired' }
    let(:custom_error_class) { Mongo::Error::SocketTimeoutError }
    let(:custom_error_message) { 'socket timed out' }

    context 'with time argument' do
      it 'raises StandardError' do
        expect do
          Mongo::Timeout.timeout(0.1) do
            sleep 1
          end
        end.to raise_error(::Timeout::Error, default_error_message)
      end
    end

    context 'with time and class arguments' do
      it 'raises the specified error class' do
        expect do
          Mongo::Timeout.timeout(0.1, custom_error_class) do
            sleep 1
          end
        end.to raise_error(custom_error_class, default_error_message)
      end
    end

    context 'with time, class, and message arguments' do
      it 'raises the specified error class with message' do
        expect do
          Mongo::Timeout.timeout(0.1, custom_error_class, custom_error_message) do
            sleep 1
          end
        end.to raise_error(custom_error_class, custom_error_message)
      end
    end
  end
end
