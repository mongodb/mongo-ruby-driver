require 'spec_helper'

describe Mongo::Protocol::Insert do

  let(:opcode) { 2002 }
  let(:db)     { TEST_DB }
  let(:coll)   { TEST_COLL }
  let(:ns)     { "#{db}.#{coll}" }
  let(:doc1)   { { :name => "Tyler" } }
  let(:doc2)   { { :name => "Brandon" } }
  let(:docs)   { [doc1, doc2 ] }
  let(:opts)   { { } }

  let(:message) do
    described_class.new(db, coll, docs, opts)
  end

  describe '#initialize' do

    it 'sets the namepsace' do
      expect(message.namespace).to eq(ns)
    end

    it 'sets the documents' do
      expect(message.documents).to eq(docs)
    end

    context 'when options are provided' do

      context 'when flags are provided' do
        let(:opts) { { :flags => [:continue_on_error] } }

        it 'sets the flags' do
          expect(message.flags).to eq(opts[:flags])
        end
      end
    end
  end

  describe '#serialize' do
    let(:bytes) { message.serialize }

    include_examples 'message with a header'

    describe 'flags' do
      let(:field) { bytes[16..19] }

      context 'when no flags are provided' do
        it 'does not set any bits' do
          expect(field).to be_int32(0)
        end
      end

      context 'when flags are provided' do
        let(:opts) { { :flags => flags } }

        context 'continue on error flag' do
          let(:flags) { [:continue_on_error] }
          it 'sets the first bit' do
            expect(field).to be_int32(1)
          end
        end
      end
    end

    describe 'namespace' do
      let(:field) { bytes[20..36] }
      it 'serializes the namespace' do
        expect(field).to be_cstring(ns)
      end
    end

    describe 'documents' do
      let(:field) { bytes[37..-1] }
      it 'serializes the documents' do
        expect(field).to be_bson_sequence(docs)
      end
    end
  end
end
