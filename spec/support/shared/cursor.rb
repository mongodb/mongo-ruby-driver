shared_context 'shared cursor' do

  let(:view_opts) { Hash.new }
  let(:view) { Mongo::CollectionView.new(collection, {}, view_opts) }

  let(:nonzero) { 1 }
  let(:b) { proc { |d| d } }

  def results(cursor_id = 0, nreturned = 5)
    [{ :cursor_id => cursor_id,
       :nreturned => nreturned,
       :docs => (0...nreturned).to_a },
     server]
  end

  let(:responses) do
    results
  end
end
