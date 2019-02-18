require 'spec_helper'

describe Mongo::Index::View do

  let(:view) do
    described_class.new(authorized_collection, options)
  end

  let(:options) do
    {}
  end

  before do
    begin
      authorized_collection.delete_many
    rescue Mongo::Error::OperationFailure
    end
    begin
      authorized_collection.indexes.drop_all
    rescue Mongo::Error::OperationFailure
    end
  end

  describe '#drop_one' do

    let(:spec) do
      { another: -1 }
    end

    before do
      view.create_one(spec, unique: true)
    end

    context 'when provided a session' do

      let(:view_with_session) do
        described_class.new(authorized_collection, session: session)
      end

      let(:client) do
        authorized_client
      end

      let(:operation) do
        view_with_session.drop_one('another_-1')
      end

      let(:failed_operation) do
        view_with_session.drop_one('_another_-1')
      end

      it_behaves_like 'an operation using a session'
      it_behaves_like 'a failed operation using a session'
    end

    context 'when the index exists' do

      let(:result) do
        view.drop_one('another_-1')
      end

      it 'drops the index' do
        expect(result).to be_successful
      end
    end

    context 'when passing a * as the name' do

      it 'raises an exception' do
        expect {
          view.drop_one('*')
        }.to raise_error(Mongo::Error::MultiIndexDrop)
      end
    end

    context 'when the collection has a write concern' do

      let(:collection) do
        authorized_collection.with(write: INVALID_WRITE_CONCERN)
      end

      let(:view_with_write_concern) do
        described_class.new(collection)
      end

      let(:result) do
        view_with_write_concern.drop_one('another_-1')
      end

      context 'when the server accepts writeConcern for the dropIndexes operation' do
        min_server_fcv '3.4'

        it 'applies the write concern' do
          expect {
            result
          }.to raise_exception(Mongo::Error::OperationFailure)
        end
      end

      context 'when the server does not accept writeConcern for the dropIndexes operation' do
        max_server_version '3.2'

        it 'does not apply the write concern' do
          expect(result).to be_successful
        end
      end
    end

    context 'when there are multiple indexes with the same key pattern' do
      min_server_fcv '3.4'

      before do
        view.create_one({ random: 1 }, unique: true)
        view.create_one({ random: 1 },
                          name: 'random_1_with_collation',
                          unique: true,
                          collation: { locale: 'en_US', strength: 2 })
      end

      context 'when a name is supplied' do

        let!(:result) do
          view.drop_one('random_1_with_collation')
        end

        let(:index_names) do
          view.collect { |model| model['name'] }
        end

        it 'returns ok' do
          expect(result).to be_successful
        end

        it 'drops the correct index' do
          expect(index_names).not_to include('random_1_with_collation')
          expect(index_names).to include('random_1')
        end
      end
    end
  end

  describe '#drop_all' do

    let(:spec) do
      { another: -1 }
    end

    before do
      view.create_one(spec, unique: true)
    end

    context 'when indexes exists' do

      let(:result) do
        view.drop_all
      end

      it 'drops the index' do
        expect(result).to be_successful
      end

      context 'when provided a session' do

        let(:view_with_session) do
          described_class.new(authorized_collection, session: session)
        end

        let(:operation) do
          view_with_session.drop_all
        end

        let(:client) do
          authorized_client
        end

        it_behaves_like 'an operation using a session'
      end

      context 'when the collection has a write concern' do

        let(:collection) do
          authorized_collection.with(write: INVALID_WRITE_CONCERN)
        end

        let(:view_with_write_concern) do
          described_class.new(collection)
        end

        let(:result) do
          view_with_write_concern.drop_all
        end

        context 'when the server accepts writeConcern for the dropIndexes operation' do
          min_server_fcv '3.4'

          it 'applies the write concern' do
            expect {
              result
            }.to raise_exception(Mongo::Error::OperationFailure)
          end
        end

        context 'when the server does not accept writeConcern for the dropIndexes operation' do
          max_server_version '3.2'

          it 'does not apply the write concern' do
            expect(result).to be_successful
          end
        end
      end
    end
  end

  describe '#create_many' do

    context 'when the indexes are created' do

      context 'when passing multi-args' do

        context 'when the index creation is successful' do

          let!(:result) do
            view.create_many(
              { key: { random: 1 }, unique: true },
              { key: { testing: -1 }, unique: true }
            )
          end

          it 'returns ok' do
            expect(result).to be_successful
          end

          context 'when provided a session' do

            let(:view_with_session) do
              described_class.new(authorized_collection, session: session)
            end

            let(:operation) do
              view_with_session.create_many(
                  { key: { random: 1 }, unique: true },
                  { key: { testing: -1 }, unique: true }
              )
            end

            let(:client) do
              authorized_client
            end

            let(:failed_operation) do
              view_with_session.create_many(
                  { key: { random: 1 }, invalid: true }
              )
            end

            it_behaves_like 'an operation using a session'
            it_behaves_like 'a failed operation using a session'
          end
        end

        context 'when collation is specified' do
          min_server_fcv '3.4'

          let(:result) do
            view.create_many(
              { key: { random: 1 },
                unique: true,
                collation: { locale: 'en_US', strength: 2 } }
            )
          end

          let(:index_info) do
            view.get('random_1')
          end

          context 'when the server supports collations' do
            min_server_fcv '3.4'

            it 'returns ok' do
              expect(result).to be_successful
            end

            it 'applies the collation to the new index' do
              result
              expect(index_info['collation']).not_to be_nil
              expect(index_info['collation']['locale']).to eq('en_US')
              expect(index_info['collation']['strength']).to eq(2)
            end
          end

          context 'when the server does not support collations' do
            max_server_version '3.2'

            it 'raises an exception' do
              expect {
                result
              }.to raise_exception(Mongo::Error::UnsupportedCollation)
            end

            context 'when a String key is used' do

              let(:result) do
                view.create_many(
                  { key: { random: 1 },
                    unique: true,
                    'collation' => { locale: 'en_US', strength: 2 } }
                )
              end

              it 'raises an exception' do
                expect {
                  result
                }.to raise_exception(Mongo::Error::UnsupportedCollation)
              end
            end
          end
        end

        context 'when the collection has a write concern' do

          let(:collection) do
            authorized_collection.with(write: INVALID_WRITE_CONCERN)
          end

          let(:view_with_write_concern) do
            described_class.new(collection)
          end

          let(:result) do
            view_with_write_concern.create_many(
                { key: { random: 1 }, unique: true },
                { key: { testing: -1 }, unique: true }
            )
          end

          context 'when the server accepts writeConcern for the createIndexes operation' do
            min_server_fcv '3.4'

            it 'applies the write concern' do
              expect {
                result
              }.to raise_exception(Mongo::Error::OperationFailure)
            end
          end

          context 'when the server does not accept writeConcern for the createIndexes operation' do
            max_server_version '3.2'

            it 'does not apply the write concern' do
              expect(result).to be_successful
            end
          end
        end
      end

      context 'when passing an array' do

        context 'when the index creation is successful' do

          let!(:result) do
            view.create_many([
                                 { key: { random: 1 }, unique: true },
                                 { key: { testing: -1 }, unique: true }
                             ])
          end

          it 'returns ok' do
            expect(result).to be_successful
          end

          context 'when provided a session' do

            let(:view_with_session) do
              described_class.new(authorized_collection, session: session)
            end

            let(:operation) do
              view_with_session.create_many([
                                             { key: { random: 1 }, unique: true },
                                             { key: { testing: -1 }, unique: true }
                                            ])
            end

            let(:failed_operation) do
              view_with_session.create_many([ { key: { random: 1 }, invalid: true }])
            end

            let(:client) do
              authorized_client
            end

            it_behaves_like 'an operation using a session'
            it_behaves_like 'a failed operation using a session'
          end
        end

        context 'when collation is specified' do

          let(:result) do
            view.create_many([
                                 { key: { random: 1 },
                                   unique: true,
                                   collation: { locale: 'en_US', strength: 2 }},
                             ])
          end

          let(:index_info) do
            view.get('random_1')
          end

          context 'when the server supports collations' do
            min_server_fcv '3.4'

            it 'returns ok' do
              expect(result).to be_successful
            end

            it 'applies the collation to the new index' do
              result
              expect(index_info['collation']).not_to be_nil
              expect(index_info['collation']['locale']).to eq('en_US')
              expect(index_info['collation']['strength']).to eq(2)
            end
          end

          context 'when the server does not support collations' do
            max_server_version '3.2'

            it 'raises an exception' do
              expect {
                result
              }.to raise_exception(Mongo::Error::UnsupportedCollation)
            end

            context 'when a String key is used' do

              let(:result) do
                view.create_many([
                                   { key: { random: 1 },
                                     unique: true,
                                     'collation' => { locale: 'en_US', strength: 2 }},
                                 ])
              end

              it 'raises an exception' do
                expect {
                  result
                }.to raise_exception(Mongo::Error::UnsupportedCollation)
              end
            end
          end
        end

        context 'when the collection has a write concern' do

          let(:collection) do
            authorized_collection.with(write: INVALID_WRITE_CONCERN)
          end

          let(:view_with_write_concern) do
            described_class.new(collection)
          end

          let(:result) do
            view_with_write_concern.create_many([
                                 { key: { random: 1 }, unique: true },
                                 { key: { testing: -1 }, unique: true }
                             ])
          end

          context 'when the server accepts writeConcern for the createIndexes operation' do
            min_server_fcv '3.4'

            it 'applies the write concern' do
              expect {
                result
              }.to raise_exception(Mongo::Error::OperationFailure)
            end
          end

          context 'when the server does not accept writeConcern for the createIndexes operation' do
            max_server_version '3.2'

            it 'does not apply the write concern' do
              expect(result).to be_successful
            end
          end
        end
      end

      context 'when index creation fails' do

        let(:spec) do
          { name: 1 }
        end

        before do
          view.create_one(spec, unique: true)
        end

        it 'raises an exception' do
          expect {
            view.create_many([{ key: { name: 1 }, unique: false }])
          }.to raise_error(Mongo::Error::OperationFailure)
        end
      end
    end
  end

  describe '#create_one' do

    context 'when the index is created' do

      let(:spec) do
        { random: 1 }
      end

      let(:result) do
        view.create_one(spec, unique: true)
      end

      it 'returns ok' do
        expect(result).to be_successful
      end

      context 'when provided a session' do

        let(:view_with_session) do
          described_class.new(authorized_collection, session: session)
        end

        let(:operation) do
          view_with_session.create_one(spec, unique: true)
        end

        let(:failed_operation) do
          view_with_session.create_one(spec, invalid: true)
        end

        let(:client) do
          authorized_client
        end

        it_behaves_like 'an operation using a session'
        it_behaves_like 'a failed operation using a session'
      end

      context 'when the collection has a write concern' do

        let(:collection) do
          authorized_collection.with(write: INVALID_WRITE_CONCERN)
        end

        let(:view_with_write_concern) do
          described_class.new(collection)
        end

        let(:result) do
          view_with_write_concern.create_one(spec, unique: true)
        end

        context 'when the server accepts writeConcern for the createIndexes operation' do
          min_server_fcv '3.4'

          it 'applies the write concern' do
            expect {
              result
            }.to raise_exception(Mongo::Error::OperationFailure)
          end
        end

        context 'when the server does not accept writeConcern for the createIndexes operation' do
          max_server_version '3.2'

          it 'does not apply the write concern' do
            expect(result).to be_successful
          end
        end
      end

      context 'when the index is created on an subdocument field' do

        let(:spec) do
          { 'sub_document.random' => 1 }
        end

        let(:result) do
          view.create_one(spec, unique: true)
        end

        it 'returns ok' do
          expect(result).to be_successful
        end
      end
    end

    context 'when index creation fails' do

      let(:spec) do
        { name: 1 }
      end

      before do
        view.create_one(spec, unique: true)
      end

      it 'raises an exception' do
        expect {
          view.create_one(spec, unique: false)
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end

    context 'when providing an index name' do

      let(:spec) do
        { random: 1 }
      end

      let!(:result) do
        view.create_one(spec, unique: true, name: 'random_name')
      end

      it 'returns ok' do
        expect(result).to be_successful
      end

      it 'defines the index with the provided name' do
        expect(view.get('random_name')).to_not be_nil
      end
    end

    context 'when providing an invalid partial index filter' do
      min_server_fcv '3.2'

      it 'raises an exception' do
        expect {
          view.create_one({'x' => 1}, partial_filter_expression: 5)
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end

    context 'when providing a valid partial index filter' do
      min_server_fcv '3.2'

      let(:expression) do
        {'a' => {'$lte' => 1.5}}
      end

      let!(:result) do
        view.create_one({'x' => 1}, partial_filter_expression: expression)
      end

      let(:indexes) do
        authorized_collection.indexes.get('x_1')
      end

      it 'returns ok' do
        expect(result).to be_successful
      end

      it 'creates an index' do
        expect(indexes).to_not be_nil
      end

      it 'passes partialFilterExpression correctly' do
        expect(indexes[:partialFilterExpression]).to eq(expression)
      end
    end
  end

  describe '#get' do

    let(:spec) do
      { random: 1 }
    end

    let!(:result) do
      view.create_one(spec, unique: true, name: 'random_name')
    end

    context 'when providing a name' do

      let(:index) do
        view.get('random_name')
      end

      it 'returns the index' do
        expect(index['name']).to eq('random_name')
      end
    end

    context 'when providing a spec' do

      let(:index) do
        view.get(random: 1)
      end

      it 'returns the index' do
        expect(index['name']).to eq('random_name')
      end
    end

    context 'when provided a session' do

      let(:view_with_session) do
        described_class.new(authorized_collection, session: session)
      end

      let(:operation) do
        view_with_session.get(random: 1)
      end

      let(:client) do
        authorized_client
      end

      it_behaves_like 'an operation using a session'
    end

    context 'when the index does not exist' do

      it 'returns nil' do
        expect(view.get(other: 1)).to be_nil
      end
    end
  end

  describe '#each' do

    context 'when the collection exists' do

      let(:spec) do
        { name: 1 }
      end

      before do
        view.create_one(spec, unique: true)
      end

      let(:indexes) do
        view.each
      end

      it 'returns all the indexes for the database' do
        expect(indexes.to_a.count).to eq(2)
      end
    end

    context 'when the collection does not exist' do
      min_server_fcv '3.0'

      let(:nonexistent_collection) do
        authorized_client[:not_a_collection]
      end

      let(:nonexistent_view) do
        described_class.new(nonexistent_collection)
      end

      it 'raises a nonexistent collection error' do
        expect {
          nonexistent_view.each.to_a
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end
  end

  describe '#normalize_models' do

    context 'when providing options' do

      let(:options) do
        {
          :key => { :name => 1 },
          :bucket_size => 5,
          :default_language => 'deutsch',
          :expire_after => 10,
          :language_override => 'language',
          :sphere_version => 1,
          :storage_engine => 'wiredtiger',
          :text_version => 2,
          :version => 1
        }
      end

      let(:models) do
        view.send(:normalize_models, [ options ], authorized_primary)
      end

      let(:expected) do
        {
          :key => { :name => 1 },
          :name => 'name_1',
          :bucketSize => 5,
          :default_language => 'deutsch',
          :expireAfterSeconds => 10,
          :language_override => 'language',
          :'2dsphereIndexVersion' => 1,
          :storageEngine => 'wiredtiger',
          :textIndexVersion => 2,
          :v => 1
        }
      end

      it 'maps the ruby options to the server options' do
        expect(models).to eq([ expected ])
      end

      context 'when using alternate names' do

        let(:extended_options) do
          options.merge!(expire_after_seconds: 5)
        end

        let(:extended_expected) do
          expected.tap { |exp| exp[:expireAfterSeconds] = 5 }
        end

        let(:models) do
          view.send(:normalize_models, [ extended_options ], authorized_primary)
        end

        it 'maps the ruby options to the server options' do
          expect(models).to eq([ extended_expected ])
        end
      end

      context 'when the server supports collations' do
        min_server_fcv '3.4'

        let(:extended_options) do
          options.merge(:collation => { locale: 'en_US' } )
        end

        let(:models) do
          view.send(:normalize_models, [ extended_options ], authorized_primary)
        end

        let(:extended_expected) do
          expected.tap { |exp| exp[:collation] = { locale: 'en_US' } }
        end

        it 'maps the ruby options to the server options' do
          expect(models).to eq([ extended_expected ])
        end
      end
    end
  end
end
