# rubocop:todo all
shared_context 'auth unit tests' do
  let(:generation_manager) do
    Mongo::Server::ConnectionPool::GenerationManager.new(server: server)
  end

  let(:pool) do
    double('pool').tap do |pool|
      allow(pool).to receive(:generation_manager).and_return(generation_manager)
    end
  end

  let(:connection) do
    Mongo::Server::Connection.new(server, SpecConfig.instance.monitoring_options.merge(
      connection_pool: pool))
  end
end
