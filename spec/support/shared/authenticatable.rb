shared_context 'authenticatable context' do
  let(:username) { 'samantha.ritter' }
  let(:password) { 'secret' }
  let(:db_name) { 'test-authenticatable' }
  let(:source) { 'source_db' }
  let(:server) { Mongo::Server.new('127.0.0.1:27017') }
  let(:auth) { described_class.new(db_name, username,
                                   { :source => source,
                                     :password => password }) }
  let(:auth_no_source) { described_class.new(db_name, username,
                                             { :password => password }) }
  let(:auth_no_opts) { described_class.new(db_name, username) }
end

shared_examples 'an authenticator' do

  describe '#db_name' do

    it 'returns a String' do
      expect(auth.db_name).to be_a(String)
    end

    it 'returns the db_name' do
      expect(auth.db_name).to eq(db_name)
    end
  end

  describe '#source' do

    context 'when a source is given' do

      it 'returns a String' do
        expect(auth.source).to be_a(String)
      end

      it 'returns the name of the given source' do
        expect(auth.source).to eq(source)
      end
    end

    context 'when a source is not given' do

      it 'returns a String' do
        expect(auth_no_source.source).to be_a(String)
      end

      it 'returns the db_name' do
        expect(auth_no_source.source).to eq(db_name)
      end
    end
  end
end
