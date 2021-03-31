# frozen_string_literal: true
# encoding: utf-8

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

  describe '#collation_enabled?' do

    context 'when the wire range includes 5' do

      let(:wire_versions) do
        0..5
      end

      it 'returns true' do
        expect(features).to be_collation_enabled
      end
    end

    context 'when the wire range does not include 5' do

      let(:wire_versions) do
        0..2
      end

      it 'returns false' do
        expect(features).to_not be_collation_enabled
      end
    end
  end

  describe '#max_staleness_enabled?' do

    context 'when the wire range includes 5' do

      let(:wire_versions) do
        0..5
      end

      it 'returns true' do
        expect(features).to be_max_staleness_enabled
      end
    end

    context 'when the wire range does not include 5' do

      let(:wire_versions) do
        0..2
      end

      it 'returns false' do
        expect(features).to_not be_max_staleness_enabled
      end
    end
  end

  describe '#find_command_enabled?' do

    context 'when the wire range includes 4' do

      let(:wire_versions) do
        0..4
      end

      it 'returns true' do
        expect(features).to be_find_command_enabled
      end
    end

    context 'when the wire range does not include 4' do

      let(:wire_versions) do
        0..2
      end

      it 'returns false' do
        expect(features).to_not be_find_command_enabled
      end
    end
  end

  describe '#list_collections_enabled?' do

    context 'when the wire range includes 3' do

      let(:wire_versions) do
        0..3
      end

      it 'returns true' do
        expect(features).to be_list_collections_enabled
      end
    end

    context 'when the wire range does not include 3' do

      let(:wire_versions) do
        0..2
      end

      it 'returns false' do
        expect(features).to_not be_list_collections_enabled
      end
    end
  end

  describe '#list_indexes_enabled?' do

    context 'when the wire range includes 3' do

      let(:wire_versions) do
        0..3
      end

      it 'returns true' do
        expect(features).to be_list_indexes_enabled
      end
    end

    context 'when the wire range does not include 3' do

      let(:wire_versions) do
        0..2
      end

      it 'returns false' do
        expect(features).to_not be_list_indexes_enabled
      end
    end
  end

  describe '#write_command_enabled?' do

    context 'when the wire range includes 2' do

      let(:wire_versions) do
        0..3
      end

      it 'returns true' do
        expect(features).to be_write_command_enabled
      end
    end

    context 'when the wire range does not include 2' do

      let(:wire_versions) do
        0..1
      end

      it 'returns false' do
        expect {
          features.check_driver_support!
        }.to raise_exception(Mongo::Error::UnsupportedFeatures)
      end
    end
  end

  describe '#scram_sha_1_enabled?' do

    context 'when the wire range includes 3' do

      let(:wire_versions) do
        0..3
      end

      it 'returns true' do
        expect(features).to be_scram_sha_1_enabled
      end
    end

    context 'when the wire range does not include 3' do

      let(:wire_versions) do
        0..2
      end

      it 'returns false' do
        expect(features).to_not be_scram_sha_1_enabled
      end
    end
  end
end
