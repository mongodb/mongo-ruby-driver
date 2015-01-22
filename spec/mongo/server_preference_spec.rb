require 'spec_helper'

describe Mongo::ServerPreference do

  describe '.get' do

    let(:local_threshold_ms) { 15 }
    let(:server_selection_timeout_ms) { 30000 }

    let(:server_pref) do
      described_class.get({ :mode => name },
                          { :local_threshold_ms => local_threshold_ms,
                            :server_selection_timeout_ms => server_selection_timeout_ms })
    end

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
      let(:server_pref) { described_class.get(:mode => name, :tag_sets => tag_sets) }

      it 'sets tag sets on the server preference object' do
        expect(server_pref.tag_sets).to eq(tag_sets)
      end

    end

    context 'local threshold ms provided' do

      let(:local_threshold_ms) { 100 }

      it 'sets local_threshold_ms on the server preference object' do
        expect(server_pref.local_threshold_ms).to eq(local_threshold_ms)
      end
    end

    context 'server selection timeout ms' do

      let(:server_selection_timeout_ms) { 100 }

      it 'sets server_selection_timeout_ms on the server preference object' do
        expect(server_pref.server_selection_timeout_ms).to eq(server_selection_timeout_ms)
      end
    end
  end
end
