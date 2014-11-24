require 'spec_helper'

describe Mongo::Logger do

  let(:logger) do
    described_class.logger
  end

  describe '.debug' do

    it 'logs a debug message' do
      expect(logger).to receive(:debug).with("mongo.query | message | runtime: 10ms")
      described_class.debug('mongo.query', 'message', '10ms')
    end
  end

  describe '.error' do

    it 'logs a error message' do
      expect(logger).to receive(:error).with("mongo.query | message | runtime: 10ms")
      described_class.error('mongo.query', 'message', '10ms')
    end
  end

  describe '.fatal' do

    it 'logs a fatal message' do
      expect(logger).to receive(:fatal).with("mongo.query | message | runtime: 10ms")
      described_class.fatal('mongo.query', 'message', '10ms')
    end
  end

  describe '.info' do

    it 'logs a info message' do
      expect(logger).to receive(:info).with("mongo.query | message | runtime: 10ms")
      described_class.info('mongo.query', 'message', '10ms')
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

  describe '.warn' do

    it 'logs a warn message' do
      expect(logger).to receive(:warn).with("mongo.query | message | runtime: 10ms")
      described_class.warn('mongo.query', 'message', '10ms')
    end
  end
end
