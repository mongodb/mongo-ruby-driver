shared_context 'operation' do
  let(:server) { double('server') }
  let(:results) { [] }
  let(:connection) do
    double('connection').tap do |connection|
      allow(connection).to receive(:send_and_receive) { [results, server] }
      allow(connection).to receive(:wire_version) { 2 }
    end
  end
  let(:client) do
    double('client').tap do |client|
      allow(client).to receive(:with_context).and_yield(connection)
    end
  end
  let(:op) { described_class.new(spec, opts) }
end

