shared_context 'server preference' do
  let(:server_pref) { described_class.new(tag_sets, acceptable_latency) }
  let(:tag_sets) { [] }
  let(:tag_set) { { 'test' => 'tag' } }
  let(:acceptable_latency) { 15 }
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

      context 'tag sets and acceptable latency are the same' do
        it 'returns true' do
          expect(server_pref).to eq(other)
        end
      end

      context 'acceptable latency is different' do
        let(:acceptable_latency) { 20 }
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
