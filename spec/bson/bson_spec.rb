# encoding: utf-8
require 'spec_helper'

describe BSON do

  shared_examples_for "serialized" do |document|
    it "should not have a nil/empty serialized value" do
      serialized   = BSON.serialize(document)
      serialized.should_not be nil
    end

    it "should deserialize to the original value" do
      serialized   = BSON.serialize(document)
      deserialized = BSON.deserialize(serialized)
      deserialized.should eq document
    end
  end

  context "when the document includes" do

    context "symbols as keys" do
      it "should convert symbol keys to strings" do
        document = BSON.serialize(:hello => "world")
        BSON.deserialize(document).should eq({"hello" => "world"})
      end
    end

    context "utf-8 values as keys" do
      it_can_be "serialized", { "schönen" => "type" }
    end

    context "a utc datetime" do
      let(:document) {  }

      it "should serialize utc times successfully" do
        document     = {"utc" => Time.now.utc}
        serialized   = BSON.serialize(document)
        deserialized = BSON.deserialize(serialized)

        # Time comparisons only work if the sub-second precision loss is equal
        document["utc"].to_s.should eq deserialized["utc"].to_s
      end
    end

    context "a float" do
      it_can_be "serialized", {"float" => 1.2}
    end

    context "a string" do
      it_can_be "serialized", {"hello" => "world"}
    end

    context "a utf-8 string" do
      it_can_be "serialized", {"utf-8" => "schönen"}
    end

    context "a symbol" do
      it_can_be "serialized", {"sym" => :s}
    end

    context "a utf-8 symbol" do
      it_can_be "serialized", {"utf-8" => "schönen".to_sym }
    end

    context "a nil/undefined" do
      it_can_be "serialized", {"null" => nil}
    end

    context "an object id" do
      it_can_be "serialized", {
        "_id" => ObjectId.from_string('4e4d66343b39b68407000001')
      }
    end

    context "a boolean" do
      it_can_be "serialized", {"true" => true}
    end

    context "a regular expression" do
      it_can_be "serialized", {"regex" => /potato/}
    end

    context "a regular expression with flags" do
      it_can_be "serialized", {"regex" => /potato/xm}
    end

    context "a utf-8 regular expression" do
      it_can_be "serialized", {"regex" => /schönen/}
    end

    context "a utf-8 regular expression with flags" do
      it_can_be "serialized", {"regex" => /schönen/xm}
    end

    context "javascript" do
      it_can_be "serialized", {
        "javascript" => BSON::Code.new("function() { alert('hello'); }")
      }
    end

    context "javascript with scope" do
      it_can_be "serialized", {
        "javascript" => BSON::Code.new("function() {}", { "a" => 1 })
      }
    end

    # context "javascript with utf-8" do
    #   it_can_be "serialized", {
    #     "javascript" => BSON::Code.new("function() { alert('schönen'); }")
    #   }
    # end

    context "javascript with scope and utf-8" do
      it_can_be "serialized", {
        "javascript" => BSON::Code.new("function() {}", { "a" => "schönen" })
      }
    end

    context "a 32-bit integer" do
      it_can_be "serialized", {"int" => 100}
    end

    context "a 64-bit integer" do
      it_can_be "serialized", {"int" => 999_999_999_999}
    end

    context "a timestamp" do
      it_can_be "serialized", {"ts" => BSON::Timestamp.new(100, 101)}
    end

    # context "a minkey value" do
    #   pending do
    #     it_can_be "serialized", { "min" => BSON::MinKey }
    #   end
    # end

    # context "a maxkey value" do
    #   pending do
    #     it_can_be "serialized", { "max" => BSON::MaxKey }
    #   end
    # end

    context "an embedded document" do
      it_can_be "serialized", {"int" => 1, "sub" => {"float" => 2.23}}
    end

    context "an embedded array" do
      it_can_be "serialized", {"num" => 2, "alpha" => ["A", "B", "C"]}
    end

    context "an embedded utf-8 array" do
      it_can_be "serialized", {"num" => 2, "alpha" => ["schönen", "tag"]}
    end

    context "a generic binary" do
      it_can_be "serialized", { "data" => BSON::Binary.new("binary", BSON::Binary::SUBTYPE_SIMPLE) }
    end

    context "an md5 binary" do
      it_can_be "serialized", { "data" => BSON::Binary.new("binary", BSON::Binary::SUBTYPE_MD5 ) }
    end

    context "a user defined binary" do
      it_can_be "serialized", { "data" => BSON::Binary.new("binary", BSON::Binary::SUBTYPE_USER_DEFINED ) }
    end

    context "a uuid binary" do
      it_can_be "serialized", { "data" => BSON::Binary.new("binary", BSON::Binary::SUBTYPE_UUID ) }
    end

  end

end
