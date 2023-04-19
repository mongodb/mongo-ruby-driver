# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Protocol::CachingHash do

  let(:hash) { described_class.new(x:1) }
  let(:bson_reg) { {x:1}.to_bson }

  describe "#to_bson" do

    context "when serializing to bson" do
      it "caches the results" do
        hash.to_bson
        expect(hash.instance_variable_get("@bytes")).to eq(bson_reg.to_s)
      end
    end

    context "when giving a non empty buffer to_bson" do

      let!(:buffer) { {z: 1}.to_bson }
      let!(:bytes) { buffer.to_s }

      it "updates the given buffer" do
        hash.to_bson(buffer)
        expect(buffer.to_s).to eq(bytes + bson_reg.to_s)
      end

      it "given buffer is not included in the cached bytes" do
        hash.to_bson(buffer)
        expect(hash.instance_variable_get("@bytes")).to eq(bson_reg.to_s)
        expect(hash.to_bson.to_s).to eq(bson_reg.to_s)
      end
    end
  end
end
