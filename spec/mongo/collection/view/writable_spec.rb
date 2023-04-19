# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Collection::View::Writable do

  let(:selector) do
    {}
  end

  let(:options) do
    {}
  end

  let(:view_collection) do
    authorized_collection
  end

  let(:view) do
    Mongo::Collection::View.new(view_collection, selector, options)
  end

  before do
    authorized_collection.delete_many
  end

  describe '#find_one_and_delete' do

    before do
      authorized_collection.insert_many([{ field: 'test1' }])
    end

    context 'when hint option is provided' do
      # Functionality on more recent servers is sufficiently covered by spec tests.
      context 'on server versions < 4.2' do
        max_server_fcv '4.0'

        it 'raises a client-side exception' do
          expect do
            view.find_one_and_delete(hint: '_id_')
          end.to raise_error(Mongo::Error::UnsupportedOption, /The MongoDB server handling this request does not support the hint option on this command./)
        end
      end

      context 'when the write concern is unacknowledged' do
        let(:view_collection) do
          client = authorized_client.with(write_concern: { w: 0 })
          client[authorized_collection.name]
        end

        context "on 4.4+ servers" do
          min_server_version '4.4'

          it "doesn't raise an error" do
            expect do
              view.find_one_and_delete(hint: '_id_')
            end.to_not raise_error(Mongo::Error::UnsupportedOption)
          end
        end

        context "on <=4.2 servers" do
          max_server_version '4.2'

          it 'raises a client-side error' do
            expect do
              view.find_one_and_delete(hint: '_id_')
            end.to raise_error(Mongo::Error::UnsupportedOption, /The hint option cannot be specified on an unacknowledged write operation/)
          end
        end
      end
    end

    context 'when a matching document is found' do

      let(:selector) do
        { field: 'test1' }
      end

      context 'when no options are provided' do

        let!(:document) do
          view.find_one_and_delete
        end

        it 'deletes the document from the database' do
          expect(view.to_a).to be_empty
        end

        it 'returns the document' do
          expect(document['field']).to eq('test1')
        end
      end

      context 'when a projection is provided' do

        let!(:document) do
          view.projection(_id: 1).find_one_and_delete
        end

        it 'deletes the document from the database' do
          expect(view.to_a).to be_empty
        end

        it 'returns the document with limited fields' do
          expect(document['field']).to be_nil
          expect(document['_id']).to_not be_nil
        end
      end

      context 'when a sort is provided' do

        let!(:document) do
          view.sort(field: 1).find_one_and_delete
        end

        it 'deletes the document from the database' do
          expect(view.to_a).to be_empty
        end

        it 'returns the document with limited fields' do
          expect(document['field']).to eq('test1')
        end
      end

      context 'when collation is specified' do

        let(:selector) do
          { name: 'BANG' }
        end

        let(:result) do
          view.find_one_and_delete
        end

        before do
          authorized_collection.insert_one(name: 'bang')
        end

        let(:options) do
          { collation: { locale: 'en_US', strength: 2 } }
        end

        context 'when the server selected supports collations' do
          min_server_fcv '3.4'

          it 'applies the collation' do
            expect(result['name']).to eq('bang')
          end
        end

        context 'when the server selected does not support collations' do
          max_server_version '3.2'

          it 'raises an exception' do
            expect {
              result
            }.to raise_exception(Mongo::Error::UnsupportedCollation)
          end

          context 'when a String key is used' do

            let(:options) do
              { 'collation' => { locale: 'en_US', strength: 2 } }
            end

            it 'raises an exception' do
              expect {
                result
              }.to raise_exception(Mongo::Error::UnsupportedCollation)
            end
          end
        end
      end

      context 'when collation is not specified' do

        let(:selector) do
          { name: 'BANG' }
        end

        let(:result) do
          view.find_one_and_delete
        end

        before do
          authorized_collection.insert_one(name: 'bang')
        end

        it 'does not apply the collation' do
          expect(result).to be_nil
        end
      end

      context 'when collation is specified as a method option' do

        let(:selector) do
          { name: 'BANG' }
        end

        let(:result) do
          view.find_one_and_delete(method_options)
        end

        before do
          authorized_collection.insert_one(name: 'bang')
        end

        let(:method_options) do
          { collation: { locale: 'en_US', strength: 2 } }
        end

        context 'when the server selected supports collations' do
          min_server_fcv '3.4'

          it 'applies the collation' do
            expect(result['name']).to eq('bang')
          end
        end

        context 'when the server selected does not support collations' do
          max_server_version '3.2'

          it 'raises an exception' do
            expect {
              result
            }.to raise_exception(Mongo::Error::UnsupportedCollation)
          end

          context 'when a String key is used' do

            let(:method_options) do
              { 'collation' => { locale: 'en_US', strength: 2 } }
            end

            it 'raises an exception' do
              expect {
                result
              }.to raise_exception(Mongo::Error::UnsupportedCollation)
            end
          end
        end
      end
    end

    context 'when no matching document is found' do

      let(:selector) do
        { field: 'test5' }
      end

      let!(:document) do
        view.find_one_and_delete
      end

      it 'returns nil' do
        expect(document).to be_nil
      end
    end
  end

  describe '#find_one_and_replace' do

    before do
      authorized_collection.insert_many([{ field: 'test1', other: 'sth' }])
    end

    context 'when hint option is provided' do
      # Functionality on more recent servers is sufficiently covered by spec tests.
      context 'on server versions < 4.2' do
        max_server_fcv '4.0'

        it 'raises a client-side exception' do
          expect do
            view.find_one_and_replace({ field: 'testing' }, { hint: '_id_' })
          end.to raise_error(Mongo::Error::UnsupportedOption, /The MongoDB server handling this request does not support the hint option on this command./)
        end
      end

      context 'when the write concern is unacknowledged' do
        let(:view_collection) do
          client = authorized_client.with(write_concern: { w: 0 })
          client[authorized_collection.name]
        end

        context "on 4.4+ servers" do
          min_server_version '4.4'

          it "doesn't raise an error" do
            expect do
              view.find_one_and_replace({ field: 'testing' }, { hint: '_id_' })
            end.to_not raise_error(Mongo::Error::UnsupportedOption)
          end
        end

        context "on <=4.2 servers" do
          max_server_version '4.2'

          it 'raises a client-side error' do
            expect do
              view.find_one_and_replace({ field: 'testing' }, { hint: '_id_' })
            end.to raise_error(Mongo::Error::UnsupportedOption, /The hint option cannot be specified on an unacknowledged write operation/)
          end
        end
      end
    end

    context 'when a matching document is found' do

      let(:selector) do
        { field: 'test1' }
      end

      context 'when no options are provided' do

        let(:document) do
          view.find_one_and_replace({ field: 'testing' })
        end

        it 'returns the original document' do
          expect(document['field']).to eq('test1')
        end
      end

      context 'when return_document options are provided' do

        let(:document) do
          view.find_one_and_replace({ field: 'testing' }, :return_document => :after)
        end

        it 'returns the new document' do
          expect(document['field']).to eq('testing')
        end

        it 'replaces the document' do
          expect(document['other']).to be_nil
        end
      end

      context 'when a projection is provided' do

        let(:document) do
          view.projection(_id: 1).find_one_and_replace({ field: 'testing' })
        end

        it 'returns the document with limited fields' do
          expect(document['field']).to be_nil
          expect(document['_id']).to_not be_nil
        end
      end

      context 'when a sort is provided' do

        let(:document) do
          view.sort(field: 1).find_one_and_replace({ field: 'testing' })
        end

        it 'returns the original document' do
          expect(document['field']).to eq('test1')
        end
      end

      context 'when collation is provided' do

        let(:selector) do
          { name: 'BANG' }
        end

        let(:result) do
          view.find_one_and_replace(name: 'doink')
        end

        before do
          authorized_collection.insert_one(name: 'bang')
        end

        let(:options) do
          { collation: { locale: 'en_US', strength: 2 } }
        end

        context 'when the server selected supports collations' do
          min_server_fcv '3.4'

          it 'applies the collation' do
            expect(result['name']).to eq('bang')
            expect(authorized_collection.find({ name: 'doink' }, limit: -1).first['name']).to eq('doink')
          end
        end

        context 'when the server selected does not support collations' do
          max_server_version '3.2'

          it 'raises an exception' do
            expect {
              result
            }.to raise_exception(Mongo::Error::UnsupportedCollation)
          end

          context 'when a String key is used' do

            let(:options) do
              { 'collation' => { locale: 'en_US', strength: 2 } }
            end

            it 'raises an exception' do
              expect {
                result
              }.to raise_exception(Mongo::Error::UnsupportedCollation)
            end
          end
        end
      end

      context 'when collation is provided as a method option' do

        let(:selector) do
          { name: 'BANG' }
        end

        let(:result) do
          view.find_one_and_replace({ name: 'doink' }, method_options)
        end

        before do
          authorized_collection.insert_one(name: 'bang')
        end

        let(:method_options) do
          { collation: { locale: 'en_US', strength: 2 } }
        end

        context 'when the server selected supports collations' do
          min_server_fcv '3.4'

          it 'applies the collation' do
            expect(result['name']).to eq('bang')
            expect(authorized_collection.find({ name: 'doink' }, limit: -1).first['name']).to eq('doink')
          end
        end

        context 'when the server selected does not support collations' do
          max_server_version '3.2'

          it 'raises an exception' do
            expect {
              result
            }.to raise_exception(Mongo::Error::UnsupportedCollation)
          end

          context 'when a String key is used' do

            let(:method_options) do
              { 'collation' => { locale: 'en_US', strength: 2 } }
            end

            it 'raises an exception' do
              expect {
                result
              }.to raise_exception(Mongo::Error::UnsupportedCollation)
            end
          end
        end
      end

      context 'when collation is not provided' do

        let(:selector) do
          { name: 'BANG' }
        end

        let(:result) do
          view.find_one_and_replace(name: 'doink')
        end

        before do
          authorized_collection.insert_one(name: 'bang')
        end

        it 'does not apply the collation' do
          expect(result).to be_nil
        end
      end
    end

    context 'when no matching document is found' do

      context 'when no upsert options are provided' do

        let(:selector) do
          { field: 'test5' }
        end

        let(:document) do
          view.find_one_and_replace({ field: 'testing' })
        end

        it 'returns nil' do
          expect(document).to be_nil
        end
      end

      context 'when upsert options are provided' do

        let(:selector) do
          { field: 'test5' }
        end

        let(:document) do
          view.find_one_and_replace({ field: 'testing' }, :upsert => true, :return_document => :after)
        end

        it 'returns the new document' do
          expect(document['field']).to eq('testing')
        end
      end
    end
  end

  describe '#find_one_and_update' do

    before do
      authorized_collection.insert_many([{ field: 'test1' }])
    end

    context 'when hint option is provided' do
      # Functionality on more recent servers is sufficiently covered by spec tests.
      context 'on server versions < 4.2' do
        max_server_fcv '4.0'

        it 'raises a client-side exception' do
          expect do
            view.find_one_and_update({ '$set' => { field: 'testing' } }, { hint: '_id_' })
          end.to raise_error(Mongo::Error::UnsupportedOption, /The MongoDB server handling this request does not support the hint option on this command./)
        end
      end

      context 'when the write concern is unacknowledged' do
        let(:view_collection) do
          client = authorized_client.with(write_concern: { w: 0 })
          client[authorized_collection.name]
        end

        context "on 4.4+ servers" do
          min_server_version '4.4'

          it "doesn't raise an error" do
            expect do
              view.find_one_and_update({ '$set' => { field: 'testing' } }, { hint: '_id_' })
            end.to_not raise_error(Mongo::Error::UnsupportedOption)
          end
        end

        context "on <=4.2 servers" do
          max_server_version '4.2'

          it 'raises a client-side error' do
            expect do
              view.find_one_and_update({ '$set' => { field: 'testing' } }, { hint: '_id_' })
            end.to raise_error(Mongo::Error::UnsupportedOption, /The hint option cannot be specified on an unacknowledged write operation/)
          end
        end
      end
    end

    context 'when a matching document is found' do

      let(:selector) do
        { field: 'test1' }
      end

      context 'when no options are provided' do

        let(:document) do
          view.find_one_and_update({ '$set' => { field: 'testing' }})
        end

        it 'returns the original document' do
          expect(document['field']).to eq('test1')
        end
      end

      context 'when return_document options are provided' do

        let(:document) do
          view.find_one_and_update({ '$set' => { field: 'testing' }}, :return_document => :after)
        end

        it 'returns the new document' do
          expect(document['field']).to eq('testing')
        end
      end

      context 'when a projection is provided' do

        let(:document) do
          view.projection(_id: 1).find_one_and_update({ '$set' => { field: 'testing' }})
        end

        it 'returns the document with limited fields' do
          expect(document['field']).to be_nil
          expect(document['_id']).to_not be_nil
        end
      end

      context 'when a sort is provided' do

        let(:document) do
          view.sort(field: 1).find_one_and_update({ '$set' => { field: 'testing' } })
        end

        it 'returns the original document' do
          expect(document['field']).to eq('test1')
        end
      end
    end

    context 'when a collation is specified' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        view.find_one_and_update({ '$set' => { other: 'doink' } })
      end

      before do
        authorized_collection.insert_one(name: 'bang')
      end

      let(:options) do
        { collation: { locale: 'en_US', strength: 2 } }
      end

      context 'when the server selected supports collations' do
        min_server_fcv '3.4'

        it 'applies the collation' do
          expect(result['name']).to eq('bang')
          expect(authorized_collection.find({ name: 'bang' }, limit: -1).first['other']).to eq('doink')
        end
      end

      context 'when the server selected does not support collations' do
        max_server_version '3.2'

        it 'raises an exception' do
          expect {
            result
          }.to raise_exception(Mongo::Error::UnsupportedCollation)
        end

        context 'when a String key is used' do

          let(:options) do
            { 'collation' => { locale: 'en_US', strength: 2 } }
          end

          it 'raises an exception' do
            expect {
              result
            }.to raise_exception(Mongo::Error::UnsupportedCollation)
          end
        end
      end
    end

    context 'when a collation is specified as a method option' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        view.find_one_and_update({ '$set' => { other: 'doink' } }, method_options)
      end

      before do
        authorized_collection.insert_one(name: 'bang')
      end

      let(:method_options) do
        { collation: { locale: 'en_US', strength: 2 } }
      end

      context 'when the server selected supports collations' do
        min_server_fcv '3.4'

        it 'applies the collation' do
          expect(result['name']).to eq('bang')
          expect(authorized_collection.find({ name: 'bang' }, limit: -1).first['other']).to eq('doink')
        end
      end

      context 'when the server selected does not support collations' do
        max_server_version '3.2'

        it 'raises an exception' do
          expect {
            result
          }.to raise_exception(Mongo::Error::UnsupportedCollation)
        end

        context 'when a String key is used' do

          let(:method_options) do
            { 'collation' => { locale: 'en_US', strength: 2 } }
          end

          it 'raises an exception' do
            expect {
              result
            }.to raise_exception(Mongo::Error::UnsupportedCollation)
          end
        end
      end
    end

    context 'when no collation is specified' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        view.find_one_and_update({ '$set' => { other: 'doink' } })
      end

      before do
        authorized_collection.insert_one(name: 'bang')
      end

      it 'does not apply the collation' do
        expect(result).to be_nil
      end
    end

    context 'when no matching document is found' do

      let(:selector) do
        { field: 'test5' }
      end

      let(:document) do
        view.find_one_and_update({ '$set' => { field: 'testing' }})
      end

      it 'returns nil' do
        expect(document).to be_nil
      end
    end
  end

  describe '#delete_many' do
    context 'when a hint option is provided' do
      # Functionality on more recent servers is sufficiently covered by spec tests.
      context 'on server versions < 3.4' do
        max_server_fcv '3.2'

        it 'raises a client-side exception' do
          expect do
            view.delete_many(hint: '_id_')
          end.to raise_error(Mongo::Error::UnsupportedOption, /The MongoDB server handling this request does not support the hint option on this command./)
        end
      end

      context 'when the write concern is unacknowledged' do
        let(:view_collection) do
          client = authorized_client.with(write_concern: { w: 0 })
          client[authorized_collection.name]
        end

        context "on 4.4+ servers" do
          min_server_version '4.4'

          it "doesn't raise an error" do
            expect do
              view.delete_many(hint: '_id_')
            end.to_not raise_error(Mongo::Error::UnsupportedOption)
          end
        end

        context "on <=4.2 servers" do
          max_server_version '4.2'

          it 'raises a client-side error' do
            expect do
              view.delete_many(hint: '_id_')
            end.to raise_error(Mongo::Error::UnsupportedOption, /The hint option cannot be specified on an unacknowledged write operation/)
          end
        end
      end
    end

    context 'when a selector was provided' do

      let(:selector) do
        { field: 'test1' }
      end

      before do
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test2' }])
      end

      let(:response) do
        view.delete_many
      end

      it 'deletes the matching documents in the collection' do
        expect(response.written_count).to eq(1)
      end
    end

    context 'when no selector was provided' do

      before do
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test2' }])
      end

      let(:response) do
        view.delete_many
      end

      it 'deletes all the documents in the collection' do
        expect(response.written_count).to eq(2)
      end
    end

    context 'when a collation is specified' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        view.delete_many
      end

      before do
        authorized_collection.insert_one(name: 'bang')
        authorized_collection.insert_one(name: 'bang')
      end

      let(:options) do
        { collation: { locale: 'en_US', strength: 2 } }
      end

      context 'when the server selected supports collations' do
        min_server_fcv '3.4'

        it 'applies the collation' do
          expect(result.written_count).to eq(2)
          expect(authorized_collection.find(name: 'bang').to_a.size).to eq(0)
        end
      end

      context 'when the server selected does not support collations' do
        max_server_version '3.2'

        it 'raises an exception' do
          expect {
            result
          }.to raise_exception(Mongo::Error::UnsupportedCollation)
        end

        context 'when a String key is used' do

          let(:options) do
            { 'collation' => { locale: 'en_US', strength: 2 } }
          end

          it 'raises an exception' do
            expect {
              result
            }.to raise_exception(Mongo::Error::UnsupportedCollation)
          end
        end
      end
    end

    context 'when a collation is specified as a method option' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        view.delete_many(method_options)
      end

      before do
        authorized_collection.insert_one(name: 'bang')
        authorized_collection.insert_one(name: 'bang')
      end

      let(:method_options) do
        { collation: { locale: 'en_US', strength: 2 } }
      end

      context 'when the server selected supports collations' do
        min_server_fcv '3.4'

        it 'applies the collation' do
          expect(result.written_count).to eq(2)
          expect(authorized_collection.find(name: 'bang').to_a.size).to eq(0)
        end
      end

      context 'when the server selected does not support collations' do
        max_server_version '3.2'

        it 'raises an exception' do
          expect {
            result
          }.to raise_exception(Mongo::Error::UnsupportedCollation)
        end

        context 'when a String key is used' do

          let(:method_options) do
            { 'collation' => { locale: 'en_US', strength: 2 } }
          end

          it 'raises an exception' do
            expect {
              result
            }.to raise_exception(Mongo::Error::UnsupportedCollation)
          end
        end
      end
    end

    context 'when a collation is not specified' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        view.delete_many
      end

      before do
        authorized_collection.insert_one(name: 'bang')
        authorized_collection.insert_one(name: 'bang')
      end

      it 'does not apply the collation' do
        expect(result.written_count).to eq(0)
        expect(authorized_collection.find(name: 'bang').to_a.size).to eq(2)
      end
    end
  end

  describe '#delete_one' do
    context 'when a hint option is provided' do
      # Functionality on more recent servers is sufficiently covered by spec tests.
      context 'on server versions < 3.4' do
        max_server_fcv '3.2'

        it 'raises a client-side exception' do
          expect do
            view.delete_one(hint: '_id_')
          end.to raise_error(Mongo::Error::UnsupportedOption, /The MongoDB server handling this request does not support the hint option on this command./)
        end
      end

      context 'when the write concern is unacknowledged' do
        let(:view_collection) do
          client = authorized_client.with(write_concern: { w: 0 })
          client[authorized_collection.name]
        end

        context "on 4.4+ servers" do
          min_server_version '4.4'

          it "doesn't raise an error" do
            expect do
              view.delete_one(hint: '_id_')
            end.to_not raise_error(Mongo::Error::UnsupportedOption)
          end
        end

        context "on <=4.2 servers" do
          max_server_version '4.2'

          it 'raises a client-side error' do
            expect do
              view.delete_one(hint: '_id_')
            end.to raise_error(Mongo::Error::UnsupportedOption, /The hint option cannot be specified on an unacknowledged write operation/)
          end
        end
      end
    end

    context 'when a selector was provided' do

      let(:selector) do
        { field: 'test1' }
      end

      before do
        authorized_collection.insert_many([
          { field: 'test1' },
          { field: 'test1' },
          { field: 'test1' }
        ])
      end

      let(:response) do
        view.delete_one
      end

      it 'deletes the first matching document in the collection' do
        expect(response.written_count).to eq(1)
      end
    end

    context 'when no selector was provided' do

      before do
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test2' }])
      end

      let(:response) do
        view.delete_one
      end

      it 'deletes the first document in the collection' do
        expect(response.written_count).to eq(1)
      end
    end

    context 'when a collation is provided' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        view.delete_one
      end

      before do
        authorized_collection.insert_one(name: 'bang')
      end

      let(:options) do
        { collation: { locale: 'en_US', strength: 2 } }
      end

      context 'when the server selected supports collations' do
        min_server_fcv '3.4'

        it 'applies the collation' do
          expect(result.written_count).to eq(1)
          expect(authorized_collection.find(name: 'bang').to_a.size).to eq(0)
        end
      end

      context 'when the server selected does not support collations' do
        max_server_version '3.2'

        it 'raises an exception' do
          expect {
            result
          }.to raise_exception(Mongo::Error::UnsupportedCollation)
        end

        context 'when a String key is used' do

          let(:options) do
            { 'collation' => { locale: 'en_US', strength: 2 } }
          end

          it 'raises an exception' do
            expect {
              result
            }.to raise_exception(Mongo::Error::UnsupportedCollation)
          end
        end
      end
    end

    context 'when a collation is provided as a method_option' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        view.delete_one(method_options)
      end

      before do
        authorized_collection.insert_one(name: 'bang')
      end

      let(:method_options) do
        { collation: { locale: 'en_US', strength: 2 } }
      end

      context 'when the server selected supports collations' do
        min_server_fcv '3.4'

        it 'applies the collation' do
          expect(result.written_count).to eq(1)
          expect(authorized_collection.find(name: 'bang').to_a.size).to eq(0)
        end
      end

      context 'when the server selected does not support collations' do
        max_server_version '3.2'

        it 'raises an exception' do
          expect {
            result
          }.to raise_exception(Mongo::Error::UnsupportedCollation)
        end

        context 'when a String key is used' do

          let(:method_options) do
            { 'collation' => { locale: 'en_US', strength: 2 } }
          end

          it 'raises an exception' do
            expect {
              result
            }.to raise_exception(Mongo::Error::UnsupportedCollation)
          end
        end
      end
    end

    context 'when a collation is not specified' do

      let(:selector) do
        {name: 'BANG'}
      end

      let(:result) do
        view.delete_one
      end

      before do
        authorized_collection.insert_one(name: 'bang')
      end

      it 'does not apply the collation' do
        expect(result.written_count).to eq(0)
        expect(authorized_collection.find(name: 'bang').to_a.size).to eq(1)
      end
    end
  end

  describe '#replace_one' do
    context 'when a hint option is provided' do
      # Functionality on more recent servers is sufficiently covered by spec tests.
      context 'on server versions < 3.4' do
        max_server_fcv '3.2'

        it 'raises a client-side exception' do
          expect do
            view.replace_one({ field: 'testing' }, { hint: '_id_' })
          end.to raise_error(Mongo::Error::UnsupportedOption, /The MongoDB server handling this request does not support the hint option on this command./)
        end
      end

      context 'when the write concern is unacknowledged' do
        let(:view_collection) do
          client = authorized_client.with(write_concern: { w: 0 })
          client[authorized_collection.name]
        end

        context "on 4.2+ servers" do
          min_server_version '4.2'

          it "doesn't raise an error" do
            expect do
              view.replace_one({ field: 'testing' }, { hint: '_id_' })
            end.to_not raise_error(Mongo::Error::UnsupportedOption)
          end
        end

        context "on <=4.0 servers" do
          max_server_version '4.0'

          it 'raises a client-side error' do
            expect do
              view.replace_one({ field: 'testing' }, { hint: '_id_' })
            end.to raise_error(Mongo::Error::UnsupportedOption, /The hint option cannot be specified on an unacknowledged write operation/)
          end
        end
      end
    end

    context 'when a selector was provided' do

      let(:selector) do
        { field: 'test1' }
      end

      before do
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test1' }])
      end

      let!(:response) do
        view.replace_one({ field: 'testing' })
      end

      let(:updated) do
        authorized_collection.find(field: 'testing').first
      end

      it 'updates the first matching document in the collection' do
        expect(response.written_count).to eq(1)
      end

      it 'updates the documents in the collection' do
        expect(updated[:field]).to eq('testing')
      end
    end

    context 'when no selector was provided' do

      before do
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test2' }])
      end

      let!(:response) do
        view.replace_one({ field: 'testing' })
      end

      let(:updated) do
        authorized_collection.find(field: 'testing').first
      end

      it 'updates the first document in the collection' do
        expect(response.written_count).to eq(1)
      end

      it 'updates the documents in the collection' do
        expect(updated[:field]).to eq('testing')
      end
    end

    context 'when upsert is false' do

      let!(:response) do
        view.replace_one({ field: 'test1' }, upsert: false)
      end

      let(:updated) do
        authorized_collection.find(field: 'test1').to_a
      end

      it 'reports that no documents were written' do
        expect(response.written_count).to eq(0)
      end

      it 'does not insert the document' do
        expect(updated).to be_empty
      end
    end

    context 'when upsert is true' do

      let!(:response) do
        view.replace_one({ field: 'test1' }, upsert: true)
      end

      let(:updated) do
        authorized_collection.find(field: 'test1').first
      end

      it 'reports that a document was written' do
        expect(response.written_count).to eq(1)
      end

      it 'inserts the document' do
        expect(updated[:field]).to eq('test1')
      end
    end

    context 'when upsert is not specified' do

      let!(:response) do
        view.replace_one({ field: 'test1' })
      end

      let(:updated) do
        authorized_collection.find(field: 'test1').to_a
      end

      it 'reports that no documents were written' do
        expect(response.written_count).to eq(0)
      end

      it 'does not insert the document' do
        expect(updated).to be_empty
      end
    end

    context 'when a collation is specified' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        view.replace_one({ name: 'doink' })
      end

      before do
        authorized_collection.insert_one(name: 'bang')
      end

      let(:options) do
        { collation: { locale: 'en_US', strength: 2 } }
      end

      context 'when the server selected supports collations' do
        min_server_fcv '3.4'

        it 'applies the collation' do
          expect(result.written_count).to eq(1)
          expect(authorized_collection.find(name: 'doink').to_a.size).to eq(1)
        end
      end

      context 'when the server selected does not support collations' do
        max_server_version '3.2'

        it 'raises an exception' do
          expect {
            result
          }.to raise_exception(Mongo::Error::UnsupportedCollation)
        end

        context 'when a String key is used' do

          let(:options) do
            { 'collation' => { locale: 'en_US', strength: 2 } }
          end

          it 'raises an exception' do
            expect {
              result
            }.to raise_exception(Mongo::Error::UnsupportedCollation)
          end
        end
      end
    end

    context 'when a collation is specified as method option' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        view.replace_one({ name: 'doink' }, method_options)
      end

      before do
        authorized_collection.insert_one(name: 'bang')
      end

      let(:method_options) do
        { collation: { locale: 'en_US', strength: 2 } }
      end

      context 'when the server selected supports collations' do
        min_server_fcv '3.4'

        it 'applies the collation' do
          expect(result.written_count).to eq(1)
          expect(authorized_collection.find(name: 'doink').to_a.size).to eq(1)
        end
      end

      context 'when the server selected does not support collations' do
        max_server_version '3.2'

        it 'raises an exception' do
          expect {
            result
          }.to raise_exception(Mongo::Error::UnsupportedCollation)
        end

        context 'when a String key is used' do

          let(:method_options) do
            { 'collation' => { locale: 'en_US', strength: 2 } }
          end

          it 'raises an exception' do
            expect {
              result
            }.to raise_exception(Mongo::Error::UnsupportedCollation)
          end
        end
      end
    end

    context 'when a collation is not specified' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        view.replace_one(name: 'doink')
      end

      before do
        authorized_collection.insert_one(name: 'bang')
      end

      it 'does not apply the collation' do
        expect(result.written_count).to eq(0)
        expect(authorized_collection.find(name: 'bang').to_a.size).to eq(1)
      end
    end
  end

  describe '#update_many' do
    context 'when a hint option is provided' do
      # Functionality on more recent servers is sufficiently covered by spec tests.
      context 'on server versions < 3.4' do
        max_server_fcv '3.2'

        it 'raises a client-side exception' do
          expect do
            view.update_many({ '$set' => { field: 'testing' } }, { hint: '_id_' })
          end.to raise_error(Mongo::Error::UnsupportedOption, /The MongoDB server handling this request does not support the hint option on this command./)
        end
      end

      context 'when the write concern is unacknowledged' do
        let(:view_collection) do
          client = authorized_client.with(write_concern: { w: 0 })
          client[authorized_collection.name]
        end

        context "on 4.2+ servers" do
          min_server_version '4.2'

          it "doesn't raise an error" do
            expect do
              view.update_many({ '$set' => { field: 'testing' } }, { hint: '_id_' })
            end.to_not raise_error(Mongo::Error::UnsupportedOption)
          end
        end

        context "on <=4.0 servers" do
          max_server_version '4.0'

          it 'raises a client-side error' do
            expect do
              view.update_many({ '$set' => { field: 'testing' } }, { hint: '_id_' })
            end.to raise_error(Mongo::Error::UnsupportedOption, /The hint option cannot be specified on an unacknowledged write operation/)
          end
        end
      end
    end

    context 'when a selector was provided' do

      let(:selector) do
        { field: 'test' }
      end

      before do
        authorized_collection.insert_many([{ field: 'test' }, { field: 'test' }])
      end

      let!(:response) do
        view.update_many('$set'=> { field: 'testing' })
      end

      let(:updated) do
        authorized_collection.find(field: 'testing').first
      end

      it 'returns the number updated' do
        expect(response.written_count).to eq(2)
      end

      it 'updates the documents in the collection' do
        expect(updated[:field]).to eq('testing')
      end
    end

    context 'when no selector was provided' do

      before do
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test2' }])
      end

      let!(:response) do
        view.update_many('$set'=> { field: 'testing' })
      end

      let(:updated) do
        authorized_collection.find
      end

      it 'returns the number updated' do
        expect(response.written_count).to eq(2)
      end

      it 'updates all the documents in the collection' do
        updated.each do |doc|
          expect(doc[:field]).to eq('testing')
        end
      end
    end

    context 'when upsert is false' do

      let(:response) do
        view.update_many({ '$set'=> { field: 'testing' } },
                           upsert: false)
      end

      let(:updated) do
        authorized_collection.find.to_a
      end

      it 'reports that no documents were updated' do
        expect(response.written_count).to eq(0)
      end

      it 'updates no documents in the collection' do
        expect(updated).to be_empty
      end
    end

    context 'when upsert is true' do

      let!(:response) do
        view.update_many({ '$set'=> { field: 'testing' } },
                           upsert: true)
      end

      let(:updated) do
        authorized_collection.find.first
      end

      it 'reports that a document was written' do
        expect(response.written_count).to eq(1)
      end

      it 'inserts a document into the collection' do
        expect(updated[:field]).to eq('testing')
      end
    end

    context 'when upsert is not specified' do

      let(:response) do
        view.update_many({ '$set'=> { field: 'testing' } })
      end

      let(:updated) do
        authorized_collection.find.to_a
      end

      it 'reports that no documents were updated' do
        expect(response.written_count).to eq(0)
      end

      it 'updates no documents in the collection' do
        expect(updated).to be_empty
      end
    end

    context 'when a collation is specified' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        view.update_many({ '$set' => { other: 'doink' } })
      end

      before do
        authorized_collection.insert_one(name: 'bang')
        authorized_collection.insert_one(name: 'baNG')
      end

      let(:options) do
        { collation: { locale: 'en_US', strength: 2 } }
      end

      context 'when the server selected supports collations' do
        min_server_fcv '3.4'

        it 'applies the collation' do
          expect(result.written_count).to eq(2)
          expect(authorized_collection.find(other: 'doink').to_a.size).to eq(2)
        end
      end

      context 'when the server selected does not support collations' do
        max_server_version '3.2'

        it 'raises an exception' do
          expect {
            result
          }.to raise_exception(Mongo::Error::UnsupportedCollation)
        end

        context 'when a String key is used' do

          let(:options) do
            { 'collation' => { locale: 'en_US', strength: 2 } }
          end

          it 'raises an exception' do
            expect {
              result
            }.to raise_exception(Mongo::Error::UnsupportedCollation)
          end
        end
      end
    end

    context 'when a collation is specified as a method option' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        view.update_many({ '$set' => { other: 'doink' } }, method_options)
      end

      before do
        authorized_collection.insert_one(name: 'bang')
        authorized_collection.insert_one(name: 'baNG')
      end

      let(:method_options) do
        { collation: { locale: 'en_US', strength: 2 } }
      end

      context 'when the server selected supports collations' do
        min_server_fcv '3.4'

        it 'applies the collation' do
          expect(result.written_count).to eq(2)
          expect(authorized_collection.find(other: 'doink').to_a.size).to eq(2)
        end
      end

      context 'when the server selected does not support collations' do
        max_server_version '3.2'

        it 'raises an exception' do
          expect {
            result
          }.to raise_exception(Mongo::Error::UnsupportedCollation)
        end

        context 'when a String key is used' do

          let(:method_options) do
            { 'collation' => { locale: 'en_US', strength: 2 } }
          end

          it 'raises an exception' do
            expect {
              result
            }.to raise_exception(Mongo::Error::UnsupportedCollation)
          end
        end
      end
    end

    context 'when collation is not specified' do

      let(:selector) do
        {name: 'BANG'}
      end

      let(:result) do
        view.update_many('$set' => {other: 'doink'})
      end

      before do
        authorized_collection.insert_one(name: 'bang')
        authorized_collection.insert_one(name: 'baNG')
      end

      it 'does not apply the collation' do
        expect(result.written_count).to eq(0)
      end
    end
  end

  describe '#update_one' do
    context 'when a hint option is provided' do
      # Functionality on more recent servers is sufficiently covered by spec tests.
      context 'on server versions < 3.4' do
        max_server_fcv '3.2'

        it 'raises a client-side exception' do
          expect do
            view.update_one({ '$set' => { field: 'testing' } }, { hint: '_id_' })
          end.to raise_error(Mongo::Error::UnsupportedOption, /The MongoDB server handling this request does not support the hint option on this command./)
        end
      end

      context 'when the write concern is unacknowledged' do
        let(:view_collection) do
          client = authorized_client.with(write_concern: { w: 0 })
          client[authorized_collection.name]
        end

        context "on 4.2+ servers" do
          min_server_version '4.2'

          it "doesn't raise an error" do
            expect do
              view.update_one({ '$set' => { field: 'testing' } }, { hint: '_id_' })
            end.to_not raise_error(Mongo::Error::UnsupportedOption)
          end
        end

        context "on <=4.0 servers" do
          max_server_version '4.0'

          it 'raises a client-side error' do
            expect do
              view.update_one({ '$set' => { field: 'testing' } }, { hint: '_id_' })
            end.to raise_error(Mongo::Error::UnsupportedOption, /The hint option cannot be specified on an unacknowledged write operation/)
          end
        end
      end
    end

    context 'when a selector was provided' do

      let(:selector) do
        { field: 'test1' }
      end

      before do
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test1' }])
      end

      let!(:response) do
        view.update_one('$set'=> { field: 'testing' })
      end

      let(:updated) do
        authorized_collection.find(field: 'testing').first
      end

      it 'updates the first matching document in the collection' do
        expect(response.written_count).to eq(1)
      end

      it 'updates the documents in the collection' do
        expect(updated[:field]).to eq('testing')
      end
    end

    context 'when no selector was provided' do

      before do
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test2' }])
      end

      let!(:response) do
        view.update_one('$set'=> { field: 'testing' })
      end

      let(:updated) do
        authorized_collection.find(field: 'testing').first
      end

      it 'updates the first document in the collection' do
        expect(response.written_count).to eq(1)
      end

      it 'updates the documents in the collection' do
        expect(updated[:field]).to eq('testing')
      end
    end

    context 'when upsert is false' do

      let(:response) do
        view.update_one({ '$set'=> { field: 'testing' } },
                          upsert: false)
      end

      let(:updated) do
        authorized_collection.find.to_a
      end

      it 'reports that no documents were updated' do
        expect(response.written_count).to eq(0)
      end

      it 'updates no documents in the collection' do
        expect(updated).to be_empty
      end
    end

    context 'when upsert is true' do

      let!(:response) do
        view.update_one({ '$set'=> { field: 'testing' } },
                          upsert: true)
      end

      let(:updated) do
        authorized_collection.find.first
      end

      it 'reports that a document was written' do
        expect(response.written_count).to eq(1)
      end

      it 'inserts a document into the collection' do
        expect(updated[:field]).to eq('testing')
      end
    end

    context 'when upsert is not specified' do

      let(:response) do
        view.update_one({ '$set'=> { field: 'testing' } })
      end

      let(:updated) do
        authorized_collection.find.to_a
      end

      it 'reports that no documents were updated' do
        expect(response.written_count).to eq(0)
      end

      it 'updates no documents in the collection' do
        expect(updated).to be_empty
      end
    end

    context 'when there is a collation specified' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        view.update_one({ '$set' => { other: 'doink' } })
      end

      before do
        authorized_collection.insert_one(name: 'bang')
      end

      let(:options) do
        { collation: { locale: 'en_US', strength: 2 } }
      end

      context 'when the server selected supports collations' do
        min_server_fcv '3.4'

        it 'applies the collation' do
          expect(result.written_count).to eq(1)
          expect(authorized_collection.find(other: 'doink').to_a.size).to eq(1)
        end
      end

      context 'when the server selected does not support collations' do
        max_server_version '3.2'

        it 'raises an exception' do
          expect {
            result
          }.to raise_exception(Mongo::Error::UnsupportedCollation)
        end

        context 'when a String key is used' do

          let(:options) do
            { 'collation' => { locale: 'en_US', strength: 2 } }
          end

          it 'raises an exception' do
            expect {
              result
            }.to raise_exception(Mongo::Error::UnsupportedCollation)
          end
        end
      end
    end

    context 'when there is a collation specified as a method option' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        view.update_one({ '$set' => { other: 'doink' } }, method_options)
      end

      before do
        authorized_collection.insert_one(name: 'bang')
      end

      let(:method_options) do
        { collation: { locale: 'en_US', strength: 2 } }
      end

      context 'when the server selected supports collations' do
        min_server_fcv '3.4'

        it 'applies the collation' do
          expect(result.written_count).to eq(1)
          expect(authorized_collection.find(other: 'doink').to_a.size).to eq(1)
        end
      end

      context 'when the server selected does not support collations' do
        max_server_version '3.2'

        it 'raises an exception' do
          expect {
            result
          }.to raise_exception(Mongo::Error::UnsupportedCollation)
        end

        context 'when a String key is used' do

          let(:method_options) do
            { 'collation' => { locale: 'en_US', strength: 2 } }
          end

          it 'raises an exception' do
            expect {
              result
            }.to raise_exception(Mongo::Error::UnsupportedCollation)
          end
        end
      end
    end

    context 'when a collation is not specified' do

      let(:selector) do
        { name: 'BANG' }
      end

      let(:result) do
        view.update_one('$set' => { other: 'doink' })
      end

      before do
        authorized_collection.insert_one(name: 'bang')
      end

      it 'does not apply the collation' do
        expect(result.written_count).to eq(0)
      end
    end
  end
end
