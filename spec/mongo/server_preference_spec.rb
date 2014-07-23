require 'spec_helper'

describe Mongo::ServerPreference do

  describe '.get' do
    let(:server_pref) { described_class.get(:mode => name) }
    let(:name) { :secondary }
    let(:tag_sets) { [{ 'test' => 'tag' }] }

    context 'name' do

      context 'primary' do
        let(:name) { :primary }

        it 'returns a server preference of class Primary' do
          expect(server_pref).to be_a(Mongo::ServerPreference::Primary)
        end
      end

      context 'primary_preferred' do
        let(:name) { :primary_preferred }

        it 'returns a server preference of class PrimaryPreferred' do
          expect(server_pref).to be_a(Mongo::ServerPreference::PrimaryPreferred)
        end
      end

      context 'secondary' do
        let(:name) { :secondary }

        it 'returns a server preference of class Secondary' do
          expect(server_pref).to be_a(Mongo::ServerPreference::Secondary)
        end
      end

      context 'secondary_preferred' do
        let(:name) { :secondary_preferred }

        it 'returns a server preference of class SecondaryPreferred' do
          expect(server_pref).to be_a(Mongo::ServerPreference::SecondaryPreferred)
        end
      end

      context 'nearest' do
        let(:name) { :nearest }

        it 'returns a server preference of class Nearest' do
          expect(server_pref).to be_a(Mongo::ServerPreference::Nearest)
        end
      end
    end

    context 'name not provided' do
      let(:server_pref) { described_class.get }

      it 'returns a server preference of class Primary' do
        expect(server_pref).to be_a(Mongo::ServerPreference::Primary)
      end
    end

    context 'tag sets provided' do
      let(:server_pref) { described_class.get(mode: name, tags: tag_sets) }

      it 'sets tag sets on the server preference object' do
        expect(server_pref.tag_sets).to eq(tag_sets)
      end

    end

    context 'acceptable latency provided' do

      let(:acceptable_latency) { 100 }

      let(:server_pref) do
        described_class.get(mode: name, tags: tag_sets, acceptable_latency: acceptable_latency)
      end

      it 'sets acceptable latency on the server preference object' do
        expect(server_pref.acceptable_latency).to eq(acceptable_latency)
      end
    end
  end
end
