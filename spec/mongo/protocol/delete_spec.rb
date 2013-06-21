require 'spec_helper'

describe Mongo::Protocol::Delete do

  let(:opcode)   { 2006 }
  let(:db)       { TEST_DB }
  let(:coll)     { TEST_COLL }
  let(:ns)       { "#{db}.#{coll}" }
  let(:selector) { { :name => "Tyler" } }
  let(:opts)     { { } }

  let(:message) do
    described_class.new(db, coll, selector, opts)
  end

  describe '#initialize' do

    it 'sets the namepsace' do
      expect(message.namespace).to eq(ns)
    end

    it 'sets the selector' do
      expect(message.selector).to eq(selector)
    end

    context 'when options are provided' do

      context 'when flags are provided' do
        let(:opts) { { :flags => [:single_remove] } }

        it 'sets the flags' do
          expect(message.flags).to eq(opts[:flags])
        end
      end
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

        context 'single remove flag' do
          let(:flags) { [:single_remove] }
          it 'sets the first bit' do
            expect(field).to be_int32(1)
          end
        end
      end
    end

    describe 'selector' do
      let(:field) { bytes[41..-1] }
      it 'serializes the selector' do
        expect(field).to be_bson(selector)
      end
    end
  end
end

