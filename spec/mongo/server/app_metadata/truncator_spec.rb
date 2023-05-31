# frozen_string_literal: true

require 'spec_helper'

# Quoted from specifications/source/mongodb-handshake/handshake.rst:
#
# Implementors SHOULD cumulatively update fields in the following order
# until the document is under the size limit:
#
# 1. Omit fields from env except env.name.
# 2. Omit fields from os except os.type.
# 3. Omit the env document entirely.
# 4. Truncate platform.

describe Mongo::Server::AppMetadata::Truncator do
  let(:truncator) { described_class.new(Marshal.load(Marshal.dump(metadata))) }

  let(:app_name) { 'application' }
  let(:driver) { { name: 'driver', version: '1.2.3' } }
  let(:os) { { type: 'Darwin', name: 'macOS', architecture: 'arm64', version: '13.4' } }
  let(:platform) { { platform: 'platform' } }
  let(:env) { { name: 'aws.lambda', region: 'region', memory_mb: 1024 } }

  let(:metadata) do
    BSON::Document.new.tap do |doc|
      doc[:application] = { name: app_name }
      doc[:driver] = driver
      doc[:os] = os
      doc[:platform] = platform
      doc[:env] = env
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

  context 'when modifying env is sufficient' do
    context 'when a single value is too long' do
      let(:env) { { name: 'name', a: 'a' * 1000, b: 'b' } }

      it 'preserves name' do
        expect(truncator.document[:env][:name]).to be == 'name'
      end

      it 'removes the too-long entry and keeps name' do
        expect(truncator.document[:env].keys).to be == %w[ name b ]
      end

      it_behaves_like 'a truncated document'
    end

    context 'when multiple values are too long' do
      let(:env) { { name: 'name', a: 'a' * 1000, b: 'b', c: 'c' * 1000, d: 'd' } }

      it 'preserves name' do
        expect(truncator.document[:env][:name]).to be == 'name'
      end

      it 'removes all other entries until size is satisifed' do
        expect(truncator.document[:env].keys).to be == %w[ name d ]
      end

      it_behaves_like 'a truncated document'
    end
  end

  context 'when modifying os is sufficient' do
    context 'when a single value is too long' do
      let(:os) { { type: 'type', a: 'a' * 1000, b: 'b' } }

      it 'truncates env' do
        expect(truncator.document[:env].keys).to be == %w[ name ]
      end

      it 'preserves type' do
        expect(truncator.document[:os][:type]).to be == 'type'
      end

      it 'removes the too-long entry and keeps type' do
        expect(truncator.document[:os].keys).to be == %w[ type b ]
      end

      it_behaves_like 'a truncated document'
    end

    context 'when multiple values are too long' do
      let(:os) { { type: 'type', a: 'a' * 1000, b: 'b', c: 'c' * 1000, d: 'd' } }

      it 'truncates env' do
        expect(truncator.document[:env].keys).to be == %w[ name ]
      end

      it 'preserves type' do
        expect(truncator.document[:os][:type]).to be == 'type'
      end

      it 'removes all other entries until size is satisifed' do
        expect(truncator.document[:os].keys).to be == %w[ type d ]
      end

      it_behaves_like 'a truncated document'
    end
  end

  context 'when truncating os is insufficient' do
    let(:env) { { name: 'n' * 1000 } }

    it 'truncates os' do
      expect(truncator.document[:os].keys).to be == %w[ type ]
    end

    it 'removes env' do
      expect(truncator.document.key?(:env)).to be false
    end

    it_behaves_like 'a truncated document'
  end

  context 'when platform is too long' do
    let(:platform) { 'n' * 1000 }

    it 'truncates os' do
      expect(truncator.document[:os].keys).to be == %w[ type ]
    end

    it 'removes env' do
      expect(truncator.document.key?(:env)).to be false
    end

    it 'truncates platform' do
      expect(truncator.document[:platform].length).to be < 1000
    end
  end
end
