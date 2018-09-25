def make_server(mode, options = {})
  tags = options[:tags] || {}
  average_round_trip_time = options[:average_round_trip_time] || 0

  ismaster = {
              'setName' => 'mongodb_set',
              'ismaster' => mode == :primary,
              'secondary' => mode != :primary,
              'tags' => tags,
              'ok' => 1
              }

  listeners = Mongo::Event::Listeners.new
  monitoring = Mongo::Monitoring.new
  address = options[:address]

  cluster = double('cluster')
  allow(cluster).to receive(:topology).and_return(topology)
  allow(cluster).to receive(:app_metadata)
  allow(cluster).to receive(:options).and_return({})
  server = Mongo::Server.new(address, cluster, monitoring, listeners, SpecConfig.instance.test_options)
  description = Mongo::Server::Description.new(address, ismaster, average_round_trip_time)
  server.tap do |s|
    allow(s).to receive(:description).and_return(description)
  end
end

shared_context 'server selector' do

  let(:max_staleness) { nil }
  let(:tag_sets) { [] }
  let(:tag_set) do
    { 'test' => 'tag' }
  end
  let(:server_tags) do
    { 'test' => 'tag', 'other' => 'tag' }
  end
  let(:primary) { make_server(:primary) }
  let(:secondary) { make_server(:secondary) }
  let(:options) { { :mode => name, :tag_sets => tag_sets, max_staleness: max_staleness } }
  let(:selector) { described_class.new(options) }
  let(:monitoring) do
    Mongo::Monitoring.new(monitoring: false)
  end
  declare_topology_double

  before do
    # Do not run monitors and do not attempt real TCP connections
    # in server selector tests
    allow_any_instance_of(Mongo::Server).to receive(:start_monitoring)
    allow_any_instance_of(Mongo::Server).to receive(:disconnect!)
  end
end

shared_examples 'a server selector mode' do

  describe '#name' do

    it 'returns the name' do
      expect(selector.name).to eq(name)
    end
  end

  describe '#slave_ok?' do

    it 'returns whether the slave_ok bit should be set' do
      expect(selector.slave_ok?).to eq(slave_ok)
    end
  end

  describe '#==' do

    context 'when mode is the same' do

      let(:other) do
        described_class.new
      end

      context 'tag sets are the same' do

        it 'returns true' do
          expect(selector).to eq(other)
        end
      end
    end

    context 'mode is different' do

      let(:other) do
        described_class.new.tap do |sel|
          allow(sel).to receive(:name).and_return(:other_mode)
        end
      end

      it 'returns false' do
        expect(selector).not_to eq(other)
      end
    end
  end
end

shared_examples 'a server selector accepting tag sets' do

  describe '#tag_sets' do

    context 'tags not provided' do

      it 'returns an empty array' do
        expect(selector.tag_sets).to be_empty
      end
    end

    context 'tag sets provided' do

      let(:tag_sets) do
        [ tag_set ]
      end

      it 'returns the tag sets' do
        expect(selector.tag_sets).to eq(tag_sets)
      end
    end
  end

  describe '#==' do
    context 'when mode is the same' do
      let(:other) { described_class.new }

      context 'tag sets are different' do
        let(:tag_sets) { { 'other' => 'tag'  } }

        it 'returns false' do
          expect(selector).not_to eq(other)
        end
      end
    end
  end
end

shared_examples 'a server selector with sensitive data in its options' do

  describe '#inspect' do

    context 'when there is sensitive data in the options' do

      let(:options) do
        Mongo::Options::Redacted.new(:mode => name, :password => 'sensitive_data')
      end

      it 'does not print out sensitive data' do
        expect(selector.inspect).not_to match(options[:password])
      end
    end
  end
end
