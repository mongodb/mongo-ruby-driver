# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Session do
  min_server_fcv '3.6'
  require_topology :replica_set, :sharded

  let(:session) do
    authorized_client.start_session(options)
  end

  let(:options) do
    {}
  end

  describe '#initialize' do

    context 'when options are provided' do

      it 'duplicates and freezes the options' do
        expect(session.options).not_to be(options)
        expect(session.options.frozen?).to be(true)
      end
    end

    it 'sets a server session with an id' do
      expect(session.session_id).to be_a(BSON::Document)
    end

    it 'sets the cluster time to nil' do
      expect(session.cluster_time).to be(nil)
    end

    it 'sets the cluster' do
      expect(session.cluster).to be(authorized_client.cluster)
    end
  end

  describe '#inspect' do

    it 'includes the Ruby object_id in the formatted string' do
      expect(session.inspect).to include(session.object_id.to_s)
    end

    it 'includes the session_id in the formatted string' do
      expect(session.inspect).to include(session.session_id.to_s)
    end

    context 'when options are provided' do

      let(:options) do
        { causal_consistency: true }
      end

      it 'includes the options in the formatted string' do
        expect(session.inspect).to include({ implicit: false,
                                             causal_consistency: true }.to_s)
      end
    end
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

      context 'when the original cluster time is less than the new cluster time' do

        let(:original_cluster_time) do
          Mongo::ClusterTime.new('clusterTime' => BSON::Timestamp.new(0, 1))
        end

        before do
          session.instance_variable_set(:@cluster_time, original_cluster_time)
          session.advance_cluster_time(new_cluster_time)
        end

        it 'sets the new cluster time' do
          expect(session.cluster_time).to eq(new_cluster_time)
        end
      end

      context 'when the original cluster time is equal or greater than the new cluster time' do

        let(:original_cluster_time) do
          Mongo::ClusterTime.new('clusterTime' => BSON::Timestamp.new(0, 6))
        end

        before do
          session.instance_variable_set(:@cluster_time, original_cluster_time)
          session.advance_cluster_time(new_cluster_time)
        end

        it 'does not update the cluster time' do
          expect(session.cluster_time).to eq(original_cluster_time)
        end
      end
    end
  end

  describe '#advance_operation_time' do

    let(:new_operation_time) do
      BSON::Timestamp.new(0, 5)
    end

    context 'when the session does not have an operation time' do

      before do
        session.advance_operation_time(new_operation_time)
      end

      it 'sets the new operation time' do
        expect(session.operation_time).to eq(new_operation_time)
      end
    end

    context 'when the session already has an operation time' do

      context 'when the original operation time is less than the new operation time' do

        let(:original_operation_time) do
          BSON::Timestamp.new(0, 1)
        end

        before do
          session.instance_variable_set(:@operation_time, original_operation_time)
          session.advance_operation_time(new_operation_time)
        end

        it 'sets the new operation time' do
          expect(session.operation_time).to eq(new_operation_time)
        end
      end

      context 'when the original operation time is equal or greater than the new operation time' do

        let(:original_operation_time) do
          BSON::Timestamp.new(0, 6)
        end

        before do
          session.instance_variable_set(:@operation_time, original_operation_time)
          session.advance_operation_time(new_operation_time)
        end

        it 'does not update the operation time' do
          expect(session.operation_time).to eq(original_operation_time)
        end
      end
    end
  end

  describe 'ended?' do

    context 'when the session has not been ended' do

      it 'returns false' do
        expect(session.ended?).to be(false)
      end
    end

    context 'when the session has been ended' do

      before do
        session.end_session
      end

      it 'returns true' do
        expect(session.ended?).to be(true)
      end
    end
  end

  describe 'end_session' do

    let!(:server_session) do
      session.instance_variable_get(:@server_session)
    end

    let(:cluster_session_pool) do
      session.cluster.session_pool
    end

    it 'returns the server session to the cluster session pool' do
      session.end_session
      expect(cluster_session_pool.instance_variable_get(:@queue)).to include(server_session)
    end

    context 'when #end_session is called multiple times' do

      before do
        session.end_session
      end

      it 'returns nil' do
        expect(session.end_session).to be_nil
      end
    end
  end

  describe '#retry_writes?' do

    context 'when the option is set to true' do

      let(:client) do
        authorized_client_with_retry_writes
      end

      it 'returns true' do
        expect(client.start_session.retry_writes?).to be(true)
      end
    end

    context 'when the option is set to false' do

      let(:client) do
        authorized_client.with(retry_writes: false)
      end

      it 'returns false' do
        expect(client.start_session.retry_writes?).to be(false)
      end
    end

    context 'when the option is not defined' do
      require_no_retry_writes

      let(:client) do
        authorized_client
      end

      it 'returns false' do
        expect(client.start_session.retry_writes?).to be(false)
      end
    end
  end

  describe '#session_id' do
    it 'returns a BSON::Document' do
      expect(session.session_id).to be_a(BSON::Document)
    end

    context 'ended session' do
      before do
        session.end_session
      end

      it 'raises SessionEnded' do
        expect do
          session.session_id
        end.to raise_error(Mongo::Error::SessionEnded)
      end
    end

    context "when the sesion is not materialized" do
      let(:session) { authorized_client.get_session(implicit: true) }

      before do
        expect(session.materialized?).to be false
      end

      it "raises SessionNotMaterialized" do

        expect do
          session.session_id
        end.to raise_error(Mongo::Error::SessionNotMaterialized)
      end
    end
  end

  describe '#txn_num' do
    it 'returns an integer' do
      expect(session.txn_num).to be_a(Integer)
    end

    context 'ended session' do
      before do
        session.end_session
      end

      it 'raises SessionEnded' do
        expect do
          session.txn_num
        end.to raise_error(Mongo::Error::SessionEnded)
      end
    end
  end

  describe '#next_txn_num' do
    it 'returns an integer' do
      expect(session.next_txn_num).to be_a(Integer)
    end

    it 'increments transaction number on each call' do
      expect(session.next_txn_num).to eq(1)
      expect(session.next_txn_num).to eq(2)
    end

    context 'ended session' do
      before do
        session.end_session
      end

      it 'raises SessionEnded' do
        expect do
          session.next_txn_num
        end.to raise_error(Mongo::Error::SessionEnded)
      end
    end
  end

  describe '#start_session' do
    context 'when block doesn\'t raise an error' do
      it 'closes the session after the block' do
        block_session = nil
        authorized_client.start_session do |session|
          expect(session.ended?).to be false
          block_session = session
        end
        expect(block_session.ended?).to be true
      end
    end

    context 'when block raises an error' do
      it 'closes the session after the block' do
        block_session = nil
        expect do
          authorized_client.start_session do |session|
            block_session = session
            raise 'This is an error!'
          end
        end.to raise_error(StandardError, 'This is an error!')
        expect(block_session.ended?).to be true
      end
    end

    context 'when block returns value' do
      it 'is returned by the function' do
        res = authorized_client.start_session do |session|
          4
        end
        expect(res).to be 4
      end
    end

    it 'returns a session with session id' do
      session = authorized_client.start_session
      session.session_id.should be_a(BSON::Document)
    end
  end
end
