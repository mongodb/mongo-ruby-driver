# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::Protocol::Registry do

  describe ".get" do

    context "when the type has a correspoding class" do

      before do
        described_class.register(Mongo::Protocol::Query::OP_CODE, Mongo::Protocol::Query)
      end

      let(:klass) do
        described_class.get(Mongo::Protocol::Query::OP_CODE, "message")
      end

      it "returns the class" do
        expect(klass).to eq(Mongo::Protocol::Query)
      end
    end

    context "when the type has no corresponding class" do

      it "raises an error" do
        expect {
          described_class.get(-100)
        }.to raise_error(Mongo::Error::UnsupportedMessageType)
      end
    end
  end
end
