require 'spec_helper'

describe Mongo::Session do

  let(:session) do
    authorized_client.start_session
  end

  describe '#advance_cluster_time' do

    let(:new_cluster_time) do
      { 'clusterTime' => BSON::Timestamp.new(0, 5) }
    end

    context 'when the session does not have a cluster time' do

      before do
        session.advance_cluster_time(new_cluster_time)
      end

      it 'sets the new cluster time' do
        expect(session.cluster_time).to eq(new_cluster_time)
      end
    end

    context 'when the session already has a cluster time' do

      context 'when the current cluster time is less than the new cluster time' do

        let(:current_cluster_time) do
          { 'clusterTime' => BSON::Timestamp.new(0, 1) }
        end

        before do
          session.instance_variable_set(:@cluster_time, current_cluster_time)
          session.advance_cluster_time(new_cluster_time)
        end

        it 'sets the new cluster time' do
          expect(session.cluster_time).to eq(new_cluster_time)
        end
      end

      context 'when the current cluster time is equal or greater than the new cluster time' do

        let(:current_cluster_time) do
          { 'clusterTime' => BSON::Timestamp.new(0, 6) }
        end

        before do
          session.instance_variable_set(:@cluster_time, current_cluster_time)
          session.advance_cluster_time(new_cluster_time)
        end

        it 'does not update the cluster time' do
          expect(session.cluster_time).to eq(current_cluster_time)
        end
      end
    end
  end
end
