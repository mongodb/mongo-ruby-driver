require 'spec_helper'

describe Mongo::Logger do

  let(:logger) do
    described_class.logger
  end

  describe '.logger' do

    context 'when no logger has been set' do

      let(:test_logger) do
        Mongo::Logger.logger
      end

      before do
        Mongo::Logger.logger = nil
      end

      after do
        Mongo::Logger.logger = test_logger
      end

      it 'returns the default logger' do
        expect(logger.level).to eq(Logger::DEBUG)
      end
    end

    context 'when a logger has been set' do

      let(:info) do
        Logger.new($stdout).tap do |log|
          log.level = Logger::INFO
        end
      end

      let(:debug) do
        Logger.new($stdout).tap do |log|
          log.level = Logger::DEBUG
        end
      end

      before do
        described_class.logger = debug
      end

      after do
        described_class.logger = info
      end

      it 'returns the provided logger' do
        expect(logger.level).to eq(Logger::DEBUG)
      end
    end
  end
end
