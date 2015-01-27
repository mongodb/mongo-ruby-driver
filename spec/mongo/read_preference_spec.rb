require 'spec_helper'

describe Mongo::ReadPreference do

  describe '.get' do

    let(:read_pref) do
      described_class.get({ :mode => name })
    end

    let(:name) { :secondary }
    let(:tag_sets) { [{ 'test' => 'tag' }] }

    context 'name' do

      context 'primary' do
        let(:name) { :primary }

        it 'returns a read preference of class Primary' do
          expect(read_pref).to be_a(Mongo::ReadPreference::Primary)
        end
      end

      context 'primary_preferred' do
        let(:name) { :primary_preferred }

        it 'returns a read preference of class PrimaryPreferred' do
          expect(read_pref).to be_a(Mongo::ReadPreference::PrimaryPreferred)
        end
      end

      context 'secondary' do
        let(:name) { :secondary }

        it 'returns a read preference of class Secondary' do
          expect(read_pref).to be_a(Mongo::ReadPreference::Secondary)
        end
      end

      context 'secondary_preferred' do
        let(:name) { :secondary_preferred }

        it 'returns a read preference of class SecondaryPreferred' do
          expect(read_pref).to be_a(Mongo::ReadPreference::SecondaryPreferred)
        end
      end

      context 'nearest' do
        let(:name) { :nearest }

        it 'returns a read preference of class Nearest' do
          expect(read_pref).to be_a(Mongo::ReadPreference::Nearest)
        end
      end
    end

    context 'name not provided' do
      let(:read_pref) { described_class.get }

      it 'returns a read preference of class Primary' do
        expect(read_pref).to be_a(Mongo::ReadPreference::Primary)
      end
    end

    context 'tag sets provided' do
      let(:read_pref) { described_class.get(:mode => name, :tag_sets => tag_sets) }

      it 'sets tag sets on the read preference object' do
        expect(read_pref.tag_sets).to eq(tag_sets)
      end

    end
  end
end
