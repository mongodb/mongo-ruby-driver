# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'BSON & command size limits' do
  # https://jira.mongodb.org/browse/RUBY-3016
  retry_test

  let(:max_document_size) { 16*1024*1024 }

  before do
    authorized_collection.delete_many
  end

  # This test uses a large document that is significantly smaller than the
  # size limit. It is a basic sanity check.
  it 'allows user-provided documents to be 15MiB' do
    document = { key: 'a' * 15*1024*1024, _id: 'foo' }

    authorized_collection.insert_one(document)
  end

  # This test uses a large document that is significantly larger than the
  # size limit. It is a basic sanity check.
  it 'fails single write of oversized documents' do
    document = { key: 'a' * 17*1024*1024, _id: 'foo' }

    lambda do
      authorized_collection.insert_one(document)
    end.should raise_error(Mongo::Error::MaxBSONSize, /The document exceeds maximum allowed BSON object size after serialization/)
  end

  # This test checks our bulk write splitting when documents are not close
  # to the limit, but where splitting is definitely required.
  it 'allows split bulk write of medium sized documents' do
    # 8 documents of 4 MiB each = 32 MiB total data, should be split over
    # either 2 or 3 bulk writes depending on how well the driver splits
    documents = []
    1.upto(8) do |index|
      documents << { key: 'a' * 4*1024*1024, _id: "in#{index}" }
    end

    authorized_collection.insert_many(documents)
    authorized_collection.count_documents.should == 8
  end

  # This test ensures that document which are too big definitely fail insertion.
  it 'fails bulk write of oversized documents' do
    documents = []
    1.upto(3) do |index|
      documents << { key: 'a' * 17*1024*1024, _id: "in#{index}" }
    end

    lambda do
      authorized_collection.insert_many(documents)
    end.should raise_error(Mongo::Error::MaxBSONSize, /The document exceeds maximum allowed BSON object size after serialization/)
    authorized_collection.count_documents.should == 0
  end

  it 'allows user-provided documents to be exactly 16MiB' do
    # The document must contain the _id field, otherwise the server will
    # add it which will increase the size of the document as persisted by
    # the server.
    document = { key: 'a' * (max_document_size - 28), _id: 'foo' }
    expect(document.to_bson.length).to eq(max_document_size)

    authorized_collection.insert_one(document)
  end

  it 'fails on the driver when a document larger than 16MiB is inserted' do
    document = { key: 'a' * (max_document_size - 27), _id: 'foo' }
    expect(document.to_bson.length).to eq(max_document_size+1)

    lambda do
      authorized_collection.insert_one(document)
    end.should raise_error(Mongo::Error::MaxBSONSize, /The document exceeds maximum allowed BSON object size after serialization/)
  end

  it 'fails on the driver when an update larger than 16MiB is performed' do
    document = { "$set" => { key: 'a' * (max_document_size - 25) } }
    expect(document.to_bson.length).to eq(max_document_size+1)

    lambda do
      authorized_collection.update_one({ _id: 'foo' }, document)
    end.should raise_error(Mongo::Error::MaxBSONSize, /The document exceeds maximum allowed BSON object size after serialization/)
  end

  it 'fails on the driver when an delete larger than 16MiB is performed' do
    document = { key: 'a' * (max_document_size - 14) }
    expect(document.to_bson.length).to eq(max_document_size+1)

    lambda do
      authorized_collection.delete_one(document)
    end.should raise_error(Mongo::Error::MaxBSONSize, /The document exceeds maximum allowed BSON object size after serialization/)
  end

  it 'fails in the driver when a document larger than 16MiB+16KiB is inserted' do
    document = { key: 'a' * (max_document_size - 27 + 16*1024), _id: 'foo' }
    expect(document.to_bson.length).to eq(max_document_size+16*1024+1)

    lambda do
      authorized_collection.insert_one(document)
    end.should raise_error(Mongo::Error::MaxBSONSize, /The document exceeds maximum allowed BSON object size after serialization/)
  end

  it 'allows bulk writes of multiple documents of exactly 16 MiB each' do
    documents = []
    1.upto(3) do |index|
      document = { key: 'a' * (max_document_size - 28), _id: "in#{index}" }
      expect(document.to_bson.length).to eq(max_document_size)
      documents << document
    end

    authorized_collection.insert_many(documents)
    authorized_collection.count_documents.should == 3
  end
end
