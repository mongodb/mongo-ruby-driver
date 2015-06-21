shared_context 'operation' do

  let(:db_name) { TEST_DB }
  let(:coll_name) { TEST_COLL }
  let(:write_concern) { Mongo::WriteConcern.get(:w => 1) }
  let(:options) { {} }

  let(:cluster_double) do
    double('cluster')
  end

  # Server doubles
  let(:secondary_server) do
    double('secondary_server').tap do |s|
      allow(s).to receive(:secondary?) { true }
      allow(s).to receive(:primary?) { false }
      allow(s).to receive(:standalone?) { false }
    end
  end
  let(:primary_server) do
    double('primary_server').tap do |s|
      allow(s).to receive(:primary?) { true }
      allow(s).to receive(:secondary?) { false }
      allow(s).to receive(:standalone?) { false }
    end
  end

  let(:features_2_4) do
    double('features').tap do |cxt|
      allow(cxt).to receive(:write_command_enabled?) { false }
    end
  end

  let(:features_2_6) do
    double('features').tap do |cxt|
      allow(cxt).to receive(:write_command_enabled?) { true }
    end
  end

  # Context doubles
  let(:primary_context) do
    double('primary_context').tap do |cxt|
      allow(cxt).to receive(:with_connection).and_yield(connection)
      allow(cxt).to receive(:server) { primary_server }
      allow(cxt).to receive(:features) { features_2_6 }
      allow(cxt).to receive(:mongos?) { false }
      allow(cxt).to receive(:primary?) { true }
      allow(cxt).to receive(:secondary?) { false }
      allow(cxt).to receive(:standalone?) { false }
      allow(cxt).to receive(:cluster) { cluster_double }
      allow(cluster_double).to receive(:single?) { false }
    end
  end
  let(:secondary_context) do
    double('secondary_context').tap do |cxt|
      allow(cxt).to receive(:with_connection).and_yield(connection)
      allow(cxt).to receive(:server) { secondary_server }
      allow(cxt).to receive(:mongos?) { false }
      allow(cxt).to receive(:features) { features_2_6 }
      allow(cxt).to receive(:secondary?) { true }
      allow(cxt).to receive(:primary?) { false }
      allow(cxt).to receive(:standalone?) { false }
      allow(cxt).to receive(:cluster) { cluster_double }
      allow(cluster_double).to receive(:single?) { false }
    end
  end
  let(:secondary_context_slave) do
    double('secondary_context').tap do |cxt|
      allow(cxt).to receive(:with_connection).and_yield(connection)
      allow(cxt).to receive(:server) { secondary_server }
      allow(cxt).to receive(:mongos?) { false }
      allow(cxt).to receive(:features) { features_2_6 }
      allow(cxt).to receive(:secondary?) { true }
      allow(cxt).to receive(:primary?) { false }
      allow(cxt).to receive(:standalone?) { false }
      allow(cxt).to receive(:cluster) { cluster_double }
      allow(cluster_double).to receive(:single?) { true }
    end
  end
  let(:primary_context_2_4_version) do
    double('primary_context').tap do |cxt|
      allow(cxt).to receive(:with_connection).and_yield(connection)
      allow(cxt).to receive(:server) { primary_server }
      allow(cxt).to receive(:mongos?) { false }
      allow(cxt).to receive(:primary?) { true }
      allow(cxt).to receive(:secondary?) { false }
      allow(cxt).to receive(:standalone?) { false }
      allow(cxt).to receive(:cluster) { cluster_double }
      allow(cluster_double).to receive(:single?) { false }
      allow(cxt).to receive(:features) { features_2_4 }
    end
  end

  let(:connection) do
    double('connection').tap do |conn|
      allow(conn).to receive(:dispatch) { [] }
    end
  end
end

