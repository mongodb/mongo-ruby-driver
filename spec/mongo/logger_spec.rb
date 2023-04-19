# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::Logger do

  let(:logger) do
    described_class.logger
  end

  around do |example|
    saved_logger = Mongo::Logger.logger

    begin
      example.run
    ensure
      Mongo::Logger.logger = saved_logger
    end
  end

  describe '.logger' do

    context 'when no logger has been set' do

      let(:test_logger) do
        Mongo::Logger.logger
      end

      before do
        Mongo::Logger.logger = nil
      end

      it 'returns the default logger' do
        expect(logger.level).to eq(Logger::INFO)
      end
    end

    context 'when a logger has been set' do

      let(:info) do
        Logger.new(STDOUT).tap do |log|
          log.level = Logger::INFO
        end
      end

      let(:debug) do
        Logger.new(STDOUT).tap do |log|
          log.level = Logger::DEBUG
        end
      end

      before do
        described_class.logger = debug
      end

      it 'returns the provided logger' do
        expect(logger.level).to eq(Logger::DEBUG)
      end
    end
  end
end
