def server(mode, options = {})
  tags = options[:tags] || {}
  round_trip_time = options[:round_trip_time] || 0

  ismaster = {
              'setName' => 'mongodb_set',
              'ismaster' => mode == :primary,
              'secondary' => mode != :primary,
              'tags' => tags,
              'ok' => 1
              }

  listeners = Mongo::Event::Listeners.new
  address = Mongo::Address.new('127.0.0.1:27017')

  server = Mongo::Server.new(address, listeners)
  description = Mongo::Server::Description.new(address, ismaster, listeners, round_trip_time)
  server.tap do |s|
    s.instance_variable_set(:@description, description)
  end
end

shared_context 'server preference' do
  let(:server_pref) do
    described_class.new(tag_sets, local_threshold_ms,
                        server_selection_timeout_ms)
  end
  let(:tag_sets) { [] }
  let(:tag_set) { { 'test' => 'tag' } }
  let(:local_threshold_ms) { 15 }
  let(:server_selection_timeout_ms) { 30000 }
  let(:primary) { server(:primary) }
  let(:secondary) { server(:secondary) }
end

shared_examples 'a server preference mode' do

  describe '#name' do

    it 'returns the name' do
      expect(server_pref.name).to eq(name)
    end
  end

  describe '#slave_ok?' do

    it 'returns whether the slave_ok bit should be set' do
      expect(server_pref.slave_ok?).to eq(slave_ok)
    end
  end

  describe '#==' do

    context 'when mode is the same' do
      let(:other) { described_class.new }

      context 'tag sets and local threshold, server selection timeout are the same' do
        it 'returns true' do
          expect(server_pref).to eq(other)
        end
      end

      context 'local threshold ms is different' do
        let(:local_threshold_ms) { 20 }
        it 'returns false' do
          expect(server_pref).not_to eq(other)
        end
      end

      context 'server selection timeout is different' do
        let(:server_selection_timeout_ms) { 20000 }
        it 'returns false' do
          expect(server_pref).not_to eq(other)
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
        expect(server_pref).not_to eq(other)
      end
    end
  end
end

shared_examples 'a server preference mode accepting tag sets' do

  describe '#tag_sets' do

    context 'tags not provided' do

      it 'returns an empty array' do
        expect(server_pref.tag_sets).to be_empty
      end
    end

    context 'tag sets provided' do
      let(:tag_sets) { [tag_set] }

      it 'returns the tag sets' do
        expect(server_pref.tag_sets).to eq(tag_sets)
      end
    end
  end

  describe '#==' do
    context 'when mode is the same' do
      let(:other) { described_class.new }

      context 'tag sets are different' do
        let(:tag_sets) { { 'other' => 'tag'  } }

        it 'returns false' do
          expect(server_pref).not_to eq(other)
        end
      end
    end
  end
end
