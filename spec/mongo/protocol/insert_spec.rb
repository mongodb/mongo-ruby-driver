require 'spec_helper'

describe Mongo::Protocol::Insert do

  let(:opcode) { 2002 }
  let(:db)     { TEST_DB }
  let(:coll)   { TEST_COLL }
  let(:ns)     { "#{db}.#{coll}" }
  let(:doc1)   { { :name => 'Tyler' } }
  let(:doc2)   { { :name => 'Brandon' } }
  let(:docs)   { [doc1, doc2] }
  let(:options)   { Hash.new }

  let(:message) do
    described_class.new(db, coll, docs, options)
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
        let(:options) { { :flags => [:continue_on_error] } }

        it 'sets the flags' do
          expect(message.flags).to eq(options[:flags])
        end
      end
    end
  end

  describe '#==' do

    context 'when the other is an insert' do

      context 'when the fields are equal' do
        let(:other) do
          described_class.new(db, coll, docs, options)
        end

        it 'returns true' do
          expect(message).to eq(other)
        end
      end

      context 'when the database is not equal' do
        let(:other) do
          described_class.new('tyler', coll, docs, options)
        end

        it 'returns false' do
          expect(message).not_to eq(other)
        end
      end

      context 'when the collection is not equal' do
        let(:other) do
          described_class.new(db, 'tyler', docs, options)
        end

        it 'returns false' do
          expect(message).not_to eq(other)
        end
      end

      context 'when the documents are not equal' do
        let(:other) do
          described_class.new(db, coll, docs[1..1], options)
        end

        it 'returns false' do
          expect(message).not_to eq(other)
        end
      end

      context 'when the options are not equal' do
        let(:other) do
          described_class.new(db, coll, docs, :flags => [:continue_on_error])
        end

        it 'returns false' do
          expect(message).not_to eq(other)
        end
      end
    end

    context 'when the other is not an insert' do
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

    describe 'flags' do
      let(:field) { bytes.to_s[16..19] }

      context 'when no flags are provided' do
        it 'does not set any bits' do
          expect(field).to be_int32(0)
        end
      end

      context 'when flags are provided' do
        let(:options) { { :flags => flags } }

        context 'continue on error flag' do
          let(:flags) { [:continue_on_error] }
          it 'sets the first bit' do
            expect(field).to be_int32(1)
          end
        end
      end
    end

    describe 'namespace' do
      let(:field) { bytes.to_s[20..36] }
      it 'serializes the namespace' do
        expect(field).to be_cstring(ns)
      end
    end

    describe 'documents' do
      let(:field) { bytes.to_s[37..-1] }
      it 'serializes the documents' do
        expect(field).to be_bson_sequence(docs)
      end
    end
  end
end
