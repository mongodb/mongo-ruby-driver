shared_context 'read preference' do
  let(:pref) { described_class.new(tag_sets, acceptable_latency) }
  let(:tag_sets) { [] }
  let(:tag_set) { { 'test' => 'tag' } }
  let(:acceptable_latency) { 15 }
  let(:primary) { node(:primary) }
  let(:secondary) { node(:secondary) }
end

shared_examples 'a filter of nodes' do
end
