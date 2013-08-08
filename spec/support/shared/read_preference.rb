shared_context 'read preference' do
  let(:pref) { described_class.new(tag_sets, acceptable_latency) }
  let(:tag_sets) { [] }
  let(:tag_set) { { 'test' => 'tag' } }
  let(:acceptable_latency) { 15 }
  let(:primary) { node(:primary) }
  let(:secondary) { node(:secondary) }
end

shared_examples 'a read preference mode' do
  describe '#name' do
    it 'returns the name' do
      expect(pref.name).to eq(name)
    end
  end

  describe 'slave_ok?' do
    it 'returns the appropriate slave_ok bit' do
      expect(pref.slave_ok?).to eq(slave_ok)
    end
  end

  describe '#tag_sets' do
    context 'tags not provided' do
      it 'returns an empty array' do
        expect(pref.tag_sets).to be_empty
      end
    end

    context 'tag sets provided' do
      let(:tag_sets) { [tag_set] }
      it 'returns the tag sets' do
        expect(pref.tag_sets).to eq(tag_sets)
      end
    end
  end

  describe '#==' do
    context 'when mode is the same' do
      let(:other) { described_class.new }

      context 'tag sets and acceptable latency are the same' do
        it 'returns true' do
          expect(pref).to eq(other)
        end
      end

      context 'tag sets are different' do
        let(:tag_sets) { { 'not' => 'eq' } }
        it 'returns false' do
          expect(pref).not_to eq(other)
        end
      end

      context 'acceptable latency is different' do
        let(:acceptable_latency) { 100 }
        it 'returns false' do
          expect(pref).not_to eq(other)
        end
      end
    end

    context 'when mode is different' do
      let(:other) do
        double('Mode').tap do |mode|
          allow(mode).to receive(:name).and_return(:other)
        end
      end

      it 'returns false' do
        expect(pref).not_to eq(other)
      end
    end
  end

  describe '#hash' do
    let(:values) { [pref.name, pref.tag_sets, pref.acceptable_latency] }
    it 'returns a hash of the name, tag_sets, and acceptable latency' do
      expect(pref.hash).to eq(values.hash)
    end
  end
end

shared_examples 'a filter of nodes' do
end
