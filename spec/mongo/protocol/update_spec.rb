# frozen_string_literal: true
# encoding: utf-8

require 'lite_spec_helper'
require 'support/shared/protocol'

describe Mongo::Protocol::Update do

  let(:opcode)   { 2001 }
  let(:db)       { SpecConfig.instance.test_db }
  let(:collection_name) { 'protocol-test' }
  let(:ns)       { "#{db}.#{collection_name}" }
  let(:selector) { { :name => 'Tyler' } }
  let(:update_doc) { { :name => 'Bob' } }
  let(:options)       { Hash.new }

  let(:message) do
    described_class.new(db, collection_name, selector, update_doc, options)
  end

  describe '#initialize' do

    it 'sets the namespace' do
      expect(message.namespace).to eq(ns)
    end

    it 'sets the selector' do
      expect(message.selector).to eq(selector)
    end

    it 'sets the update document' do
      expect(message.update).to eq(update_doc)
    end
  end

  describe '#==' do

    context 'when the other is an update' do

      context 'when the fields are equal' do
        let(:other) do
          described_class.new(db, collection_name, selector, update_doc, options)
        end

        it 'returns true' do
          expect(message).to eq(other)
        end
      end

      context 'when the database is not equal' do
        let(:other) do
          described_class.new('tyler', collection_name, selector, update_doc, options)
        end

        it 'returns false' do
          expect(message).not_to eq(other)
        end
      end

      context 'when the collection is not equal' do
        let(:other) do
          described_class.new(db, 'tyler', selector, update_doc, options)
        end

        it 'returns false' do
          expect(message).not_to eq(other)
        end
      end

      context 'when the selector is not equal' do
        let(:other) do
          described_class.new(db, collection_name, { :a => 1 }, update_doc, options)
        end

        it 'returns false' do
          expect(message).not_to eq(other)
        end
      end

      context 'when the update document is not equal' do
        let(:other) do
          described_class.new(db, collection_name, selector, { :a => 1 }, options)
        end

        it 'returns false' do
          expect(message).not_to eq(other)
        end
      end

      context 'when the options are not equal' do
        let(:other) do
          described_class.new(db, collection_name, selector, update_doc,
                              :flags => :upsert)
        end

        it 'returns false' do
          expect(message).not_to eq(other)
        end
      end
    end

    context 'when the other is not a query' do
      let(:other) do
        expect(message).not_to eq('test')
      end
    end
  end

  describe '#hash' do
    let(:values) do
      message.send(:fields).map do |field|
        message.instance_variable_get(field[:name])
      end
    end

    it 'returns a hash of the field values' do
      expect(message.hash).to eq(values.hash)
    end
  end

  describe '#replyable?' do

    it 'returns false' do
      expect(message).to_not be_replyable
    end
  end

  describe '#serialize' do
    let(:bytes) { message.serialize }

    include_examples 'message with a header'

    describe 'zero' do
      let(:field) { bytes.to_s[16..19] }

      it 'serializes a zero' do
        expect(field).to be_int32(0)
      end
    end

    describe 'namespace' do
      let(:field) { bytes.to_s[20..36] }
      it 'serializes the namespace' do
        expect(field).to be_cstring(ns)
      end
    end

    describe 'flags' do
      let(:field) { bytes.to_s[37..40] }

      context 'when no flags are provided' do
        it 'does not set any bits' do
          expect(field).to be_int32(0)
        end
      end

      context 'when flags are provided' do
        let(:options) { { :flags => flags } }

        context 'upsert flag' do
          let(:flags) { [:upsert] }
          it 'sets the first bit' do
            expect(field).to be_int32(1)
          end
        end

        context 'multi update' do
          let(:flags) { [:multi_update] }
          it 'sets the second bit' do
            expect(field).to be_int32(2)
          end
        end
      end
    end

    describe 'selector' do
      let(:field) { bytes.to_s[41..61] }
      it 'serializes the selector' do
        expect(field).to be_bson(selector)
      end
    end

    describe 'update' do
      let(:field) { bytes.to_s[62..80] }
      it 'serializes the update' do
        expect(field).to be_bson(update_doc)
      end
    end
  end

  describe '#registry' do

    context 'when the class is loaded' do

      it 'registers the op code in the Protocol Registry' do
        expect(Mongo::Protocol::Registry.get(described_class::OP_CODE)).to be(described_class)
      end

      it 'creates an #op_code instance method' do
        expect(message.op_code).to eq(described_class::OP_CODE)
      end
    end
  end
end
