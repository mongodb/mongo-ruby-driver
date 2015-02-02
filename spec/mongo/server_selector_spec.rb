require 'spec_helper'

describe Mongo::ServerSelector do

  describe '.get' do

    let(:read_pref) do
      described_class.get({ :mode => name })
    end

    let(:name) { :secondary }
    let(:tag_sets) { [{ 'test' => 'tag' }] }

    context 'when the mode is primary' do
      let(:name) { :primary }

      it 'returns a read preference of class Primary' do
        expect(read_pref).to be_a(Mongo::ServerSelector::Primary)
      end
    end

    context 'when the mode is primary_preferred' do
      let(:name) { :primary_preferred }

      it 'returns a read preference of class PrimaryPreferred' do
        expect(read_pref).to be_a(Mongo::ServerSelector::PrimaryPreferred)
      end
    end

    context 'when the mode is secondary' do
      let(:name) { :secondary }

      it 'returns a read preference of class Secondary' do
        expect(read_pref).to be_a(Mongo::ServerSelector::Secondary)
      end
    end

    context 'when the mode is secondary_preferred' do
      let(:name) { :secondary_preferred }

      it 'returns a read preference of class SecondaryPreferred' do
        expect(read_pref).to be_a(Mongo::ServerSelector::SecondaryPreferred)
      end
    end

    context 'when the mode is nearest' do
      let(:name) { :nearest }

      it 'returns a read preference of class Nearest' do
        expect(read_pref).to be_a(Mongo::ServerSelector::Nearest)
      end
    end

    context 'when a mode is not provided' do
      let(:read_pref) { described_class.get }

      it 'returns a read preference of class Primary' do
        expect(read_pref).to be_a(Mongo::ServerSelector::Primary)
      end
    end

    context 'when tag sets provided' do
      let(:read_pref) { described_class.get(:mode => name, :tag_sets => tag_sets) }

      it 'sets tag sets on the read preference object' do
        expect(read_pref.tag_sets).to eq(tag_sets)
      end
    end
  end
end
