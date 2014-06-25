shared_context 'operation' do

  let(:write_concern) { Mongo::WriteConcern::Mode.get(:w => 1) }
  let(:opts) { {} }

  let(:collection) do
    double('collection').tap do |coll|
      allow(coll).to receive(:name) { 'test-coll' }
      allow(coll).to receive(:database) do
        double('database').tap do |db|
          allow(db).to receive(:name) { 'TEST_DB'}
        end
      end
    end
  end

  # Server doubles
  let(:secondary_server) do
    double('secondary_server').tap do |s|
      allow(s).to receive(:secondary?) { true }
    end
  end
  let(:primary_server) do
    double('primary_server').tap do |s|
      allow(s).to receive(:secondary?) { false }
      allow(s).to receive(:context) { primary_context }
    end
  end

  # Context doubles
  let(:primary_context) do
    double('primary_context').tap do |cxt|
      allow(cxt).to receive(:with_connection).and_yield(connection)
      allow(cxt).to receive(:server) { primary_server }
      allow(cxt).to receive(:wire_version) { 2 }
      allow(cxt).to receive(:primary?) { true }
    end
  end
  let(:secondary_context) do
    double('secondary_context').tap do |cxt|
      allow(cxt).to receive(:with_connection).and_yield(connection)
      allow(cxt).to receive(:server) do
        secondary_server
      end
    end
  end
  let(:primary_context_2_4_version) do
    double('primary_context').tap do |cxt|
      allow(cxt).to receive(:with_connection).and_yield(connection)
      allow(cxt).to receive(:server) { primary_server }
      allow(cxt).to receive(:wire_version) { 1 }
      allow(cxt).to receive(:primary?) { true }
    end
  end

  let(:connection) do
    double('connection').tap do |conn|
      allow(conn).to receive(:dispatch) { [] }
    end
  end
end

