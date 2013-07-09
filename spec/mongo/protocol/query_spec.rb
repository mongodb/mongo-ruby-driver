require 'spec_helper'

describe Mongo::Protocol::Query do

  let(:opcode)   { 2004 }
  let(:db)       { TEST_DB }
  let(:coll)     { TEST_COLL }
  let(:ns)       { "#{db}.#{coll}" }
  let(:selector) { { :name => 'Tyler' } }
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
        let(:opts) { { :flags => [:slave_ok] } }

        it 'sets the flags' do
          expect(message.flags).to eq(opts[:flags])
        end
      end

      context 'when a limit is provided' do
        let(:opts) { { :limit => 5 } }

        it 'sets the limit' do
          expect(message.limit).to eq(opts[:limit])
        end
      end

      context 'when a skip is provided' do
        let(:opts) { { :skip => 13 } }

        it 'sets the flags' do
          expect(message.skip).to eq(opts[:skip])
        end
      end

      context 'when a projection is provided' do
        let(:opts) { { :project => { :_id => 0 } } }

        it 'sets the projection' do
          expect(message.project).to eq(opts[:project])
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

        context 'tailable cursor flag' do
          let(:flags) { [:tailable_cursor] }
          it 'sets the second bit' do
            expect(field).to be_int32(2)
          end
        end

        context 'slave ok flag' do
          let(:flags) { [:slave_ok] }
          it 'sets the third bit' do
            expect(field).to be_int32(4)
          end
        end

        context 'oplog replay flag' do
          let(:flags) { [:oplog_replay] }
          it 'sets the fourth bit' do
            expect(field).to be_int32(8)
          end
        end

        context 'no cursor timeout flag' do
          let(:flags) { [:no_cursor_timeout] }
          it 'sets the fifth bit' do
            expect(field).to be_int32(16)
          end
        end

        context 'await data flag' do
          let(:flags) { [:await_data] }
          it 'sets the sixth bit' do
            expect(field).to be_int32(32)
          end
        end

        context 'exhaust flag' do
          let(:flags) { [:exhaust] }
          it 'sets the seventh bit' do
            expect(field).to be_int32(64)
          end
        end

        context 'partial flag' do
          let(:flags) { [:partial] }
          it 'sets the eigth bit' do
            expect(field).to be_int32(128)
          end
        end

        context 'multiple flags' do
          let(:flags) { [:await_data, :slave_ok] }
          it 'sets the correct bits' do
            expect(field).to be_int32(36)
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

    describe 'skip' do
      let(:field) { bytes[37..40] }

      context 'when no skip is provided' do
        it 'serializes a zero' do
          expect(field).to be_int32(0)
        end
      end

      context 'when skip is provided' do
        let(:opts) { { :skip => 5 } }
        let(:query) do
          described_class.new(db, coll, selector, opts)
        end

        it 'serializes the skip' do
          expect(field).to be_int32(opts[:skip])
        end
      end
    end

    describe 'limit' do
      let(:field) { bytes[41..44] }

      context 'when no limit is provided' do
        it 'serializes a zero' do
          expect(field).to be_int32(0)
        end
      end

      context 'when limit is provided' do
        let(:opts) { { :limit => 123 } }
        it 'serializes the limit' do
          expect(field).to be_int32(opts[:limit])
        end
      end
    end

    describe 'selector' do
      let(:field) { bytes[45..65] }
      it 'serializes the selector' do
        expect(field).to be_bson(selector)
      end
    end

    describe 'project' do
      let(:field) { bytes[66..-1] }
      context 'when no projection is provided' do
        it 'does not serialize a projection' do
          expect(field).to be_empty
        end
      end

      context 'when projection is provided' do
        let(:opts) { { :project => projection } }
        let(:projection) { { :_id => 0 } }

        it 'serializes the projection' do
          expect(field).to be_bson(projection)
        end
      end
    end
  end
end
