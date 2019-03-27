require 'lite_spec_helper'
require 'support/shared/protocol'

describe Mongo::Protocol::Msg do

  let(:opcode) { 2013 }
  let(:flags)     { [] }
  let(:options)   { {} }
  let(:global_args)     { { '$db' => SpecConfig.instance.test_db, ping: 1 } }
  let(:sections)   { [ ] }

  let(:message) do
    described_class.new(flags, options, global_args, *sections)
  end

  let(:deserialized) do
    Mongo::Protocol::Message.deserialize(StringIO.new(message.serialize.to_s))
  end

  describe '#initialize' do

    it 'adds the global_args to the sections' do
      expect(message.sections[0]).to eq(type: 0, payload: global_args)
    end

    context 'when flag bits are provided' do

      context 'when valid flags are provided' do

        let(:flags) { [:more_to_come] }

        it 'sets the flags' do
          expect(message.flags).to eq(flags)
        end
      end

      context 'when flags are not provided' do

        let(:flags) { nil }

        it 'sets the flags to []' do
          expect(message.flags).to eq([])
        end
      end

      context 'when an invalid flag is provided' do

        let(:flags) { [:checksum_present] }

        let(:flag_bytes) { message.serialize.to_s[16..19] }

        it 'sets the flags' do
          expect(message.flags).to eq([:checksum_present])
        end

        it 'only serializes the valid flags' do
          expect(flag_bytes).to be_int32(1)
        end
      end
    end

    context 'with user-provided and driver-generated keys in global_args' do
      let(:global_args) do
        { 'ping' => 1, 'lsid' => '__lsid__', 'a' => 'b', '$clusterTime' => '__ct__',
          'signature' => '__signature__', 'd' => 'f'}
      end

      it 'reorders global_args for better logging' do
        expect(message.payload[:command].keys).to eq(%w(ping a d lsid $clusterTime signature))
      end
    end
  end

  describe '#==' do

    context 'when the other is a msg' do

      context 'when the fields are equal' do

        let(:other) do
          described_class.new(flags, options, global_args)
        end

        it 'returns true' do
          expect(message).to eq(other)
        end
      end

      context 'when the flags are not equal' do

        let(:other) do
          described_class.new([:more_to_come], options, global_args)
        end

        it 'returns false' do
          expect(message).not_to eq(other)
        end
      end

      context 'when the global_args are not equal' do

        let(:other) do
          described_class.new(flags, nil, { '$db'=> SpecConfig.instance.test_db, ismaster: 1 })
        end

        it 'returns false' do
          expect(message).not_to eq(other)
        end
      end
    end

    context 'when the other is not a msg' do

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

    context 'when the :more_to_come flag is set' do

      let(:flags) { [:more_to_come] }

      it 'returns false' do
        expect(message).to_not be_replyable
      end
    end

    context 'when the :more_to_come flag is not set' do

      it 'returns true' do
        expect(message).to be_replyable
      end
    end
  end

  describe '#serialize' do

    let(:bytes) do
      message.serialize
    end

    let(:flag_bytes) { bytes.to_s[16..19] }
    let(:payload_type) { bytes.to_s[20] }
    let(:payload_bytes) { bytes.to_s[21..-1] }
    let(:global_args) { { ping: 1 } }

    include_examples 'message with a header'

    context 'when flags are provided' do

      context 'when checksum_present is provided' do

        let(:flags) do
          [:checksum_present]
        end

        it 'sets the flag bits' do
          expect(flag_bytes).to be_int32(1)
        end
      end

      context 'when more_to_come is provided' do

        let(:flags) do
          [:more_to_come]
        end

        it 'sets the flag bits' do
          expect(flag_bytes).to be_int32(2)
        end
      end
    end

    context 'when no flag is provided' do

      let(:flags) do
        nil
      end

      it 'sets the flag bits to 0' do
        expect(flag_bytes).to be_int32(0)
      end
    end

    context 'when global args are provided' do

      it 'sets the payload type' do
        expect(payload_type).to eq(0.chr)
      end

      it 'serializes the global arguments' do
        expect(payload_bytes).to be_bson(global_args)
      end
    end

    context 'when additional sections are provided' do

      let(:sections) do
        [ section ]
      end

      context 'when an invalid payload type is specified' do

        let(:section) do
          { type: 2,
            payload: { identifier: 'documents',
                       sequence: [ { a: 1 } ] } }
        end

        it 'raises an exception' do
          expect {
            message.serialize
          }.to raise_exception(Mongo::Error::UnknownPayloadType)
        end
      end

      context 'when a 0 payload type is specified' do

        let(:section) do
          { type: 0, payload: { ismaster: 1 } }
        end

        let(:section_payload_type) { bytes.to_s[36] }
        let(:section_bytes) { bytes.to_s[37..-1] }

        it 'sets the payload type' do
          expect(section_payload_type).to eq(0.chr)
        end

        it 'serializes the section' do
          expect(section_bytes).to be_bson(section[:payload])
        end
      end

      context 'when a no payload type is specified' do

        let(:section) do
          { payload: { ismaster: 1 } }
        end

        let(:section_payload_type) { bytes.to_s[36] }
        let(:section_bytes) { bytes.to_s[37..-1] }

        it 'sets the payload type as 0' do
          expect(section_payload_type).to eq(0.chr)
        end

        it 'serializes the section' do
          expect(section_bytes).to be_bson(section[:payload])
        end
      end

      context 'when a 1 payload type is specified' do

        let(:section) do
          { type: 1,
            payload: { identifier: 'documents',
                       sequence: [ { a: 1 } ] } }
        end

        let(:section_payload_type) { bytes.to_s[36] }
        let(:section_size) { bytes.to_s[37..40] }
        let(:section_identifier) { bytes.to_s[41..50] }
        let(:section_bytes) { bytes.to_s[51..-1] }

        it 'sets the payload type' do
          expect(section_payload_type).to eq(1.chr)
        end

        it 'sets the section size' do
          expect(section_size).to be_int32(26)
        end

        it 'serializes the section identifier' do
          expect(section_identifier).to eq("documents#{BSON::NULL_BYTE}")
        end

        it 'serializes the section bytes' do
          expect(section_bytes).to be_bson({ a: 1 })
        end

        context 'when two sections are specified' do

          let(:sections) do
            [ section1, section2 ]
          end

          let(:section1) do
            { type: 1,
              payload: { identifier: 'documents',
                         sequence: [ { a: 1 } ] } }
          end

          let(:section2) do
            { type: 1,
              payload: { identifier: 'updates',
                         sequence: [ {:q => { :bar => 1 },
                                      :u => { :$set => { :bar => 2 } },
                                      :multi => true,
                                      :upsert => false } ] } }
          end

          let(:section1_payload_type) { bytes.to_s[36] }
          let(:section1_size) { bytes.to_s[37..40] }
          let(:section1_identifier) { bytes.to_s[41..50] }
          let(:section1_bytes) { bytes.to_s[51..62] }

          it 'sets the first payload type' do
            expect(section1_payload_type).to eq(1.chr)
          end

          it 'sets the first section size' do
            expect(section1_size).to be_int32(26)
          end

          it 'serializes the first section identifier' do
            expect(section1_identifier).to eq("documents#{BSON::NULL_BYTE}")
          end

          it 'serializes the first section bytes' do
            expect(section1_bytes).to be_bson({ a: 1 })
          end

          let(:section2_payload_type) { bytes.to_s[63] }
          let(:section2_size) { bytes.to_s[64..67] }
          let(:section2_identifier) { bytes.to_s[68..75] }
          let(:section2_bytes) { bytes.to_s[76..-1] }

          it 'sets the second payload type' do
            expect(section2_payload_type).to eq(1.chr)
          end

          it 'sets the second section size' do
            expect(section2_size).to be_int32(79)
          end

          it 'serializes the second section identifier' do
            expect(section2_identifier).to eq("updates#{BSON::NULL_BYTE}")
          end

          it 'serializes the second section bytes' do
            expect(section2_bytes).to be_bson(section2[:payload][:sequence][0])
          end
        end
      end

      context 'when the sections are mixed types and payload type 1 comes before type 0' do

        let(:section1) do
          { type: 1,
            payload: { identifier: 'documents', sequence: [ { 'a' => 1 }]}}
        end

        let(:section2) do
          { type: 0, payload: { 'b' => 2 } }
        end

        let(:sections) do
          [ section1, section2 ]
        end

        it 'serializes all sections' do
          expect(deserialized.documents).to eq([ BSON::Document.new(global_args), { 'a' => 1 }, { 'b' => 2 }])
        end
      end
    end

    context 'when the validating_keys option is true with payload 1' do

      let(:sections) do
        [ section ]
      end

      let(:section) do
        { type: 1, payload: { identifier: 'documents', sequence: [ { '$b' => 2 } ] } }
      end

      let(:options) do
        { validating_keys: true }
      end

      it 'checks the sequence document keys' do
        expect {
          message.serialize
        }.to raise_exception(BSON::String::IllegalKey)
      end
    end

    context 'when the validating_keys option is false with payload 1' do

      let(:sections) do
        [ section ]
      end

      let(:section) do
        { type: 1, payload: { identifier: 'documents', sequence: [ { '$b' => 2 } ] } }
      end

      let(:options) do
        { validating_keys: false }
      end

      it 'does not check the sequence document keys' do
        expect(message.serialize).to be_a(BSON::ByteBuffer)
      end
    end
  end

  describe '#deserialize' do

    context 'when the payload type is valid' do

      it 'deserializes the message' do
        expect(deserialized.documents).to eq([ BSON::Document.new(global_args) ])
      end
    end

    context 'when the payload type is not valid' do

      let(:invalid_payload_message) do
        message.serialize.to_s.tap do |s|
          s[20] = 5.chr
        end
      end

      it 'raises an exception' do
        expect {
          Mongo::Protocol::Message.deserialize(StringIO.new(invalid_payload_message))
        }.to raise_exception(Mongo::Error::UnknownPayloadType)
      end
    end
  end

  describe '#payload' do

    context 'when the msg only contains a payload type 0' do

      it 'creates a payload with the command' do
        expect(message.payload[:command_name]).to eq('ping')
        expect(message.payload[:database_name]).to eq(SpecConfig.instance.test_db)
        expect(message.payload[:command]).to eq('ping' => 1)
        expect(message.payload[:request_id]).to eq(message.request_id)
      end
    end

    context 'when the contains a payload type 1' do

      let(:section) do
        { type: 1,
          payload: { identifier: 'documents',
                     sequence: [ { a: 1 } ] } }
      end

      let(:global_args) do
        { '$db' => SpecConfig.instance.test_db,
          'insert' => 'foo',
          'ordered' => true
        }
      end

      let(:sections) do
        [ section ]
      end

      let(:expected_command_doc) do
        {
            'insert' => 'foo',
            'documents' => [{ 'a' => 1 }],
            'ordered' => true
        }
      end

      it 'creates a payload with the command' do
        expect(message.payload[:command_name]).to eq('insert')
        expect(message.payload[:database_name]).to eq(SpecConfig.instance.test_db)
        expect(message.payload[:command]).to eq(expected_command_doc)
        expect(message.payload[:request_id]).to eq(message.request_id)
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
