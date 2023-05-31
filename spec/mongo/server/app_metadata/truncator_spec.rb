# frozen_string_literal: true

require 'spec_helper'

# Quoted from specifications/source/mongodb-handshake/handshake.rst:
#
# Drivers MUST validate these values and truncate or omit driver provided
# values if necessary. Implementors SHOULD prioritize fields to preserve in
# this order:
#
# 1. application.name
# 2. driver.*
# 3. os.type
# 4. env.name
# 5. os.* (except type)
# 6. env.* (except name)
# 7. platform

describe Mongo::Server::AppMetadata::Truncator do
  let(:truncator) { described_class.new(Marshal.load(Marshal.dump(metadata))) }

  let(:app_name) { 'application' }
  let(:driver) { { name: 'driver', version: '1.2.3' } }
  let(:os) { { type: 'Darwin', name: 'macOS', architecture: 'arm64', version: '13.4' } }
  let(:platform) { { platform: 'platform' } }
  let(:env) { { name: 'aws.lambda', region: 'region', memory_mb: 1024 } }
  let(:extra) { nil }

  let(:metadata) do
    BSON::Document.new.tap do |doc|
      doc[:application] = { name: app_name } if app_name
      doc[:driver] = driver if driver
      doc[:os] = os if os
      doc[:platform] = platform if platform
      doc[:env] = env if env
      doc[:__extra__] = extra if extra
    end
  end

  let(:untruncated_length) { metadata.to_bson.to_s.length }
  let(:truncated_length) { truncator.document.to_bson.to_s.length }

  shared_examples_for 'a truncated document' do
    it 'is shorter' do
      expect(truncated_length).to be < untruncated_length
    end

    it 'is not be longer than the maximum document size' do
      expect(truncated_length).to be <= described_class::MAX_DOCUMENT_SIZE
    end
  end

  describe 'MAX_DOCUMENT_SIZE' do
    it 'is 512 bytes' do
      # This test is an additional check that MAX_DOCUMENT_SIZE
      # has not been accidentially changed.
      expect(described_class::MAX_DOCUMENT_SIZE).to be == 512
    end
  end

  context 'when document does not need truncating' do
    it 'does not truncate anything' do
      expect(truncated_length).to be == untruncated_length
    end
  end

  context 'when modifying the platform is sufficient' do
    context 'when truncating the platform is sufficient' do
      let(:platform) { 'a' * 1000 }

      it 'truncates the platform' do
        expect(truncator.document[:platform].length).to be < 1000
      end

      it_behaves_like 'a truncated document'
    end

    context 'when the platform must be removed' do
      # env is higher priority than platform, and will require platform to
      # be resolved before truncating or removing anything in env.
      let(:env) { { name: 'abc', a: 'a' * 1000 } }

      it 'removes the platform' do
        expect(truncator.document.key?(:platform)).to be false
      end

      it_behaves_like 'a truncated document'
    end
  end

  context 'when modifying env is required' do
    context 'when truncating a single key is sufficient' do
      let(:env) { { name: 'abc', a: 'a' * 1000, b: '123' } }

      it 'truncates that key' do
        expect(truncator.document[:env].keys.sort).to be == %w[ a b name ]
        expect(truncator.document[:env][:a].length).to be < 1000
        expect(truncator.document[:env][:name]).to be == 'abc'
        expect(truncator.document[:env][:b]).to be == '123'
      end

      it_behaves_like 'a truncated document'
    end

    context 'when removing a key is required' do
      let(:env) { { name: 'abc', a: 'a' * 1000, b: 'b' * 1000 } }

      it 'removes the key' do
        expect(truncator.document[:env].keys.sort).to be == %w[ b name ]
        expect(truncator.document[:env][:b].length).to be < 1000
        expect(truncator.document[:env][:name]).to be == 'abc'
      end

      it_behaves_like 'a truncated document'
    end
  end

  context 'when modifying os is required' do
    context 'when env is problematic' do
      let(:env) { { name: 'abc', a: 'a' * 1000, b: 'b' * 1000 } }
      let(:os) { { type: 'abc', a: 'a' * 1000 } }

      it 'modifies env first' do
        expect(truncator.document[:env].keys.sort).to be == %w[ name ]
        expect(truncator.document[:os].keys.sort).to be == %w[ a type ]
      end

      it_behaves_like 'a truncated document'
    end

    context 'when truncating a single value is sufficient' do
      let(:os) { { type: 'abc', a: 'a' * 1000, b: '123' } }

      it 'truncates that key' do
        expect(truncator.document[:os].keys.sort).to be == %w[ a b type ]
        expect(truncator.document[:os][:a].length).to be < 1000
        expect(truncator.document[:os][:type]).to be == 'abc'
        expect(truncator.document[:os][:b]).to be == '123'
      end

      it_behaves_like 'a truncated document'
    end

    context 'when removing a key is required' do
      let(:os) { { type: 'abc', a: 'a' * 1000, b: 'b' * 1000 } }

      it 'removes the key' do
        expect(truncator.document[:os].keys.sort).to be == %w[ b type ]
        expect(truncator.document[:os][:b].length).to be < 1000
        expect(truncator.document[:os][:type]).to be == 'abc'
      end

      it_behaves_like 'a truncated document'
    end
  end

  context 'when modifying env.name is required' do
    let(:env) { { name: 'n' * 1000, a: 'a' * 1000, b: 'b' * 1000 } }
    let(:os) { { type: '123', a: 'a' * 1000, b: 'b' * 1000 } }

    context 'when truncating env.name is sufficient' do
      it 'truncates env.name' do
        expect(truncator.document[:env].keys.sort).to be == %w[ name ]
        expect(truncator.document[:os].keys.sort).to be == %w[ type ]
        expect(truncator.document[:env][:name].length).to be < 1000
      end

      it_behaves_like 'a truncated document'
    end

    context 'when removing env.name is required' do
      let(:os) { { type: 'n' * 1000 } }

      it 'removes env' do
        expect(truncator.document.key?(:env)).to be false
      end

      it_behaves_like 'a truncated document'
    end
  end

  context 'when modifying os.type is required' do
    let(:os) { { type: 'n' * 1000, a: 'a' * 1000, b: 'b' * 1000 } }

    context 'when truncating os.type is sufficient' do
      it 'truncates os.type' do
        expect(truncator.document.key?(:env)).to be false
        expect(truncator.document[:os].keys.sort).to be == %w[ type ]
        expect(truncator.document[:os][:type].length).to be < 1000
      end

      it_behaves_like 'a truncated document'
    end

    context 'when removing os.type is required' do
      let(:app_name) { 'n' * 1000 }

      it 'removes os' do
        expect(truncator.document.key?(:os)).to be false
      end

      it_behaves_like 'a truncated document'
    end
  end

  context 'when modifying driver is required' do
    context 'when truncating a single key is sufficient' do
      let(:driver) { { name: 'd' * 1000, version: '1.2.3' } }

      it 'truncates that key' do
        expect(truncator.document.key?(:os)).to be false
        expect(truncator.document[:driver].keys.sort).to be == %w[ name version ]
        expect(truncator.document[:driver][:version].length).to be < 1000
      end

      it_behaves_like 'a truncated document'
    end

    context 'when removing a key is required' do
      let(:driver) { { name: 'd' * 1000, version: 'v' * 1000 } }

      it 'removes the key' do
        expect(truncator.document.key?(:os)).to be false
        expect(truncator.document[:driver].keys.sort).to be == %w[ version ]
        expect(truncator.document[:driver][:version].length).to be < 1000
      end

      it_behaves_like 'a truncated document'
    end
  end

  context 'when modifying application.name is required' do
    context 'when truncating application.name is sufficient' do
      let(:app_name) { 'n' * 1000 }

      it 'truncates the name' do
        expect(truncator.document.key?(:driver)).to be false
        expect(truncator.document[:application][:name].length).to be < 1000
      end

      it_behaves_like 'a truncated document'
    end

    context 'when removing application.name is required' do
      let(:app_name) { 'n' * 1000 }
      let(:extra) { 'n' * described_class::MAX_DOCUMENT_SIZE }

      it 'removes the application key' do
        expect(truncator.document.key?(:driver)).to be false
        expect(truncator.document.key?(:application)).to be false
      end
    end
  end
end
