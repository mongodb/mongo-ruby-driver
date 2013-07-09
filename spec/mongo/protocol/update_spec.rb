require 'spec_helper'

describe Mongo::Protocol::Update do

  let(:opcode)   { 2001 }
  let(:db)       { TEST_DB }
  let(:coll)     { TEST_COLL }
  let(:ns)       { "#{db}.#{coll}" }
  let(:selector) { { :name => 'Tyler' } }
  let(:update_doc) { { :name => 'Bob' } }
  let(:opts)       { { } }
  let(:message) do
    described_class.new(db, coll, selector, update_doc, opts)
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

  describe '#serialize' do
    let(:bytes) { message.serialize }

    include_examples 'message with a header'

    describe 'zero' do
      let(:field) { bytes[16..19] }

      it 'serializes a zero' do
        expect(field).to be_int32(0)
      end
    end

    describe 'namespace' do
      let(:field) { bytes[20..36] }
      it 'serializes the namespace' do
        expect(field).to be_cstring(ns)
      end
    end

    describe 'flags' do
      let(:field) { bytes[37..40] }

      context 'when no flags are provided' do
        it 'does not set any bits' do
          expect(field).to be_int32(0)
        end
      end

      context 'when flags are provided' do
        let(:opts) { { :flags => flags } }

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
      let(:field) { bytes[41..61] }
      it 'serializes the selector' do
        expect(field).to be_bson(selector)
      end
    end

    describe 'update' do
      let(:field) { bytes[62..80] }
      it 'serializes the update' do
        expect(field).to be_bson(update_doc)
      end
    end
  end
end

