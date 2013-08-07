shared_context 'shared find_and_modify' do

  let(:replacement) { { :f_name => 'Emilie' } }
  let(:update) { { :$set => { :f_name => 'Emilie' } } }

  let(:selector) { { } }
  let(:scope_opts) { { } }
  let(:scope) do
    database = double('database', :name => TEST_DB)
    collection = Mongo::Collection.new(database, TEST_COLL)
    allow(collection).to receive(:full_namespace).
      and_return("#{database.name}.#{collection.name}")
    Mongo::Scope.new(collection, selector, scope_opts).tap do |scope|
      allow(scope).to receive(:cluster).and_return { cluster }
    end
  end

  let(:cluster) { double('cluster', :execute => results) }

  let(:fm_opts) { {} }
  let(:fm_op) { Mongo::Operation::FindAndModify.new(scope, fm_opts) }

  let(:value) do
    { '_id' => '123', 'name' => 'Emily' }
  end

  let(:last_error_object) do
    { 'n' => 1,
      'connectionId' => 1,
      'err' => 'null',
      'ok' => 1
    }
  end

  let(:results) do
    { 'lastErrorObject' => last_error_object,
      'value' => value,
    }
  end
end
