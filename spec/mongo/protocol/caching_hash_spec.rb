# frozen_string_literal: true
# encoding: utf-8

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

  describe "#needs_validation?" do

    before do
      hash.to_bson(BSON::ByteBuffer.new, validation)
    end

    let(:needs_validation?) do
      hash.send(:needs_validation?, validating_keys)
    end

    context "when calling to_bson without validation" do
      let(:validation) { false }

      context "validating_keys is true" do
        let(:validating_keys) { true }

        it "is true" do
          expect(needs_validation?).to be true
        end
      end

      context "validating_keys is false" do
        let(:validating_keys) { false }

        it "is false" do
          expect(needs_validation?).to be false
        end
      end
    end

    context "when calling to_bson with validation" do
      let(:validation) { true }

      [true, false].each do |b|
        context "validating_keys is #{b}" do
          let(:validating_keys) { b }

          it "is false" do
            expect(needs_validation?).to be false
          end
        end
      end
    end
  end
end
