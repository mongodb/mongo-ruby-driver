require 'spec_helper'

describe Mongo::Server::Description::Features do

  describe '#initialize' do

    context 'when the server wire version range is the same' do

      let(:features) do
        described_class.new(0..3)
      end

      it 'sets the server wire version range' do
        expect(features.server_wire_versions).to eq(0..3)
      end
    end

    context 'when the server wire version range is higher' do

      it 'raises an exception' do
        expect {
          described_class.new(0..4)
        }.to raise_error(Mongo::Server::Description::Features::Unsupported)
      end
    end

    context 'when the server wire version range is lower' do

      let(:features) do
        described_class.new(0..2)
      end

      it 'sets the server wire version range' do
        expect(features.server_wire_versions).to eq(0..2)
      end
    end
  end

  describe '#list_collections_enabled?' do

    context 'when the wire range includes 3' do

      let(:features) do
        described_class.new(0..3)
      end

      it 'returns true' do
        expect(features).to be_list_collections_enabled
      end
    end

    context 'when the wire range does not include 3' do

      let(:features) do
        described_class.new(0..2)
      end

      it 'returns false' do
        expect(features).to_not be_list_collections_enabled
      end
    end
  end

  describe '#list_indexes_enabled?' do

    context 'when the wire range includes 3' do

      let(:features) do
        described_class.new(0..3)
      end

      it 'returns true' do
        expect(features).to be_list_indexes_enabled
      end
    end

    context 'when the wire range does not include 3' do

      let(:features) do
        described_class.new(0..2)
      end

      it 'returns false' do
        expect(features).to_not be_list_indexes_enabled
      end
    end
  end

  describe '#write_command_enabled?' do

    context 'when the wire range includes 2' do

      let(:features) do
        described_class.new(0..3)
      end

      it 'returns true' do
        expect(features).to be_write_command_enabled
      end
    end

    context 'when the wire range does not include 2' do

      let(:features) do
        described_class.new(0..1)
      end

      it 'returns false' do
        expect(features).to_not be_write_command_enabled
      end
    end
  end

  describe '#scram_sha_1_enabled?' do

    context 'when the wire range includes 3' do

      let(:features) do
        described_class.new(0..3)
      end

      it 'returns true' do
        expect(features).to be_scram_sha_1_enabled
      end
    end

    context 'when the wire range does not include 3' do

      let(:features) do
        described_class.new(0..2)
      end

      it 'returns false' do
        expect(features).to_not be_scram_sha_1_enabled
      end
    end
  end
end
