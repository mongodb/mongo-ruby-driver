# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Server::Description::Features do

  let(:features) do
    described_class.new(wire_versions, default_address)
  end

  describe '#initialize' do

    context 'when the server wire version range is the same' do

      let(:wire_versions) do
        0..3
      end

      it 'sets the server wire version range' do
        expect(features.server_wire_versions).to eq(0..3)
      end
    end

    context 'when the server wire version range min is higher' do

      let(:wire_versions) do
        described_class::DRIVER_WIRE_VERSIONS.max+1..described_class::DRIVER_WIRE_VERSIONS.max+2
      end

      it 'raises an exception' do
        expect {
          features.check_driver_support!
        }.to raise_error(Mongo::Error::UnsupportedFeatures)
      end
    end

    if described_class::DEPRECATED_WIRE_VERSIONS.any?
      context 'when the max server wire version range is deprecated' do
        before do
          Mongo::Deprecations.clear!
        end

        let(:wire_versions) do
          (described_class::DEPRECATED_WIRE_VERSIONS.min - 1)..described_class::DEPRECATED_WIRE_VERSIONS.max
        end

        it 'issues a deprecation warning' do
          expect {
            features.check_driver_support!
          }.to change {
            Mongo::Deprecations.warned?("wire_version:#{default_address}")
          }.from(false).to(true)
        end
      end
    end

    context 'when the server wire version range max is higher' do

      let(:wire_versions) do
        0..4
      end

      it 'sets the server wire version range' do
        expect(features.server_wire_versions).to eq(0..4)
      end
    end

    context 'when the server wire version range max is lower' do

      let(:wire_versions) do
        described_class::DRIVER_WIRE_VERSIONS.min-2..described_class::DRIVER_WIRE_VERSIONS.min-1
      end

      it 'raises an exception' do
        expect {
          features.check_driver_support!
        }.to raise_error(Mongo::Error::UnsupportedFeatures)
      end
    end

    context 'when the server wire version range max is lower' do

      let(:wire_versions) do
        0..2
      end

      it 'sets the server wire version range' do
        expect(features.server_wire_versions).to eq(0..2)
      end
    end
  end

  describe '#get_more_comment_enabled?' do
    context 'when the wire range includes 9' do

      let(:wire_versions) do
        0..9
      end

      it 'returns true' do
        expect(features).to be_get_more_comment_enabled
      end
    end

    context 'when the wire range does not include 9' do

      let(:wire_versions) do
        0..8
      end

      it 'returns false' do
        expect(features).to_not be_get_more_comment_enabled
      end
    end
  end
end
