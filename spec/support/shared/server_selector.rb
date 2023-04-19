# frozen_string_literal: true
# rubocop:todo all

shared_context 'server selector' do

  let(:max_staleness) { nil }
  let(:tag_sets) { [] }
  let(:hedge) { nil }

  let(:tag_set) do
    { 'test' => 'tag' }
  end
  let(:server_tags) do
    { 'test' => 'tag', 'other' => 'tag' }
  end
  let(:primary) { make_server(:primary) }
  let(:secondary) { make_server(:secondary) }
  let(:mongos) do
    make_server(:mongos).tap do |server|
      expect(server.mongos?).to be true
    end
  end
  let(:unknown) do
    make_server(:unknown).tap do |server|
      expect(server.unknown?).to be true
    end
  end
  let(:server_selection_timeout_options) do
    {
      server_selection_timeout: 0.1,
    }
  end
  let(:options) do
    {
      mode: name,
      tag_sets: tag_sets,
      max_staleness: max_staleness,
      hedge: hedge,
    }
  end
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

  describe '#secondary_ok?' do

    it 'returns whether the secondary_ok bit should be set' do
      expect(selector.secondary_ok?).to eq(secondary_ok)
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

shared_examples 'a server selector accepting hedge' do
  describe '#initialize' do
    context 'when hedge is not provided' do
      it 'initializes successfully' do
        expect do
          selector
        end.not_to raise_error
      end
    end

    context 'when hedge is not a Hash' do
      let(:hedge) { true }

      it 'raises an exception' do
        expect do
          selector
        end.to raise_error(Mongo::Error::InvalidServerPreference, /`hedge` value \(true\) is invalid/)
      end
    end

    context 'when hedge is an empty Hash' do
      let(:hedge) { {} }

      it 'raises an exception' do
        expect do
          selector
        end.to raise_error(Mongo::Error::InvalidServerPreference, /`hedge` value \({}\) is invalid/)
      end
    end

    context 'when hedge is a Hash with data' do
      let(:hedge) { { enabled: false } }

      it 'initializes successfully' do
        expect do
          selector
        end.not_to raise_error
      end
    end
  end

  describe '#hedge' do
    context 'when hedge is not provided' do
      it 'returns nil' do
        expect(selector.hedge).to be_nil
      end
    end

    context 'when hedge is a Hash with data' do
      let(:hedge) { { enabled: false } }

      it 'returns the same Hash' do
        expect(selector.hedge).to eq({ enabled: false })
      end
    end
  end

  describe '#==' do
    let(:other_selector) { described_class.new(hedge: { enabled: false }) }

    context 'when hedges are the same' do
      let(:hedge) { { enabled: false } }

      it 'returns true' do
        expect(selector).to eq(other_selector)
      end
    end

    context 'when hedges are different' do
      let(:hedge) { { enabled: true } }

      it 'returns false' do
        expect(selector).not_to eq(other_selector)
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
