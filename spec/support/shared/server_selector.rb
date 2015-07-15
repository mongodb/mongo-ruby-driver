def server(mode, options = {})
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
  address = Mongo::Address.new('127.0.0.1:27017')

  server = Mongo::Server.new(address, double('cluster'), monitoring, listeners, TEST_OPTIONS)
  description = Mongo::Server::Description.new(address, ismaster, average_round_trip_time)
  server.tap do |s|
    allow(s).to receive(:description).and_return(description)
  end
end

shared_context 'server selector' do

  let(:tag_sets) { [] }
  let(:tag_set) do
    { 'test' => 'tag' }
  end
  let(:server_tags) do
    { 'test' => 'tag', 'other' => 'tag' }
  end
  let(:primary) { server(:primary) }
  let(:secondary) { server(:secondary) }
  let(:selector) { described_class.new(:mode => name, :tag_sets => tag_sets) }
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
        double('selectable').tap do |mode|
          allow(mode).to receive(:name).and_return(:other)
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
