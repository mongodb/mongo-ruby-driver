require 'spec_helper'

describe Mongo::NodePreference do

  describe '.get' do
    let(:read_pref) { described_class.get(name) }
    let(:name) { :secondary }
    let(:tag_sets) { [{ 'test' => 'tag' }] }

    context 'name' do

      context 'primary' do
        let(:name) { :primary }

        it 'returns a node preference of class Primary' do
          expect(read_pref).to be_a(Mongo::NodePreference::Primary)
        end
      end

      context 'primary_preferred' do
        let(:name) { :primary_preferred }

        it 'returns a node preference of class PrimaryPreferred' do
          expect(read_pref).to be_a(Mongo::NodePreference::PrimaryPreferred)
        end
      end

      context 'secondary' do
        let(:name) { :secondary }

        it 'returns a node preference of class Secondary' do
          expect(read_pref).to be_a(Mongo::NodePreference::Secondary)
        end
      end

      context 'secondary_preferred' do
        let(:name) { :secondary_preferred }

        it 'returns a node preference of class SecondaryPreferred' do
          expect(read_pref).to be_a(Mongo::NodePreference::SecondaryPreferred)
        end
      end

      context 'nearest' do
        let(:name) { :nearest }

        it 'returns a node preference of class Nearest' do
          expect(read_pref).to be_a(Mongo::NodePreference::Nearest)
        end
      end
    end

    context 'name not provided' do
      let(:read_pref) { described_class.get }

      it 'returns a node preference of class Primary' do
        expect(read_pref).to be_a(Mongo::NodePreference::Primary)
      end
    end

    context 'tag sets provided' do
      let(:read_pref) { described_class.get(name, tag_sets) }

      it 'sets tag sets on the node preference object' do
        expect(read_pref.tag_sets).to eq(tag_sets)
      end

    end

    context 'acceptable latency provided' do
      let(:acceptable_latency) { 100 }
      let(:read_pref) { described_class.get(name, tag_sets, acceptable_latency) }

      it 'sets acceptable latency on the node preference object' do
        expect(read_pref.acceptable_latency).to eq(acceptable_latency)
      end
    end
  end
end