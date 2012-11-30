# encoding: utf-8
require "spec_helper"
#BSON::BSON_CODER = BSON::BSON_RUBY

describe BSON do
  shared_examples_for "a serializable bson document" do
    it "deserializes the document" do
      BSON.deserialize(raw).should eq doc
    end

    it "serializes the document" do
      BSON.serialize(doc).should eq raw.force_encoding('binary')
    end
  end

  it "coerces symbol keys to strings" do
    bson = BSON.serialize(:hello => "world")
    BSON.deserialize(bson).should eq({"hello" => "world"})
  end

  context "simple document" do
    it_behaves_like "a serializable bson document" do
      let(:raw) { "\x16\x00\x00\x00\x02hello\x00\x06\x00\x00\x00world\x00\x00" }
      let(:doc) { { "hello" => "world" } }
    end
  end

  context "float" do
    it_behaves_like "a serializable bson document" do
      let(:raw) { "\x14\x00\x00\x00\x01float\x00333333\xF3?\x00" }
      let(:doc) { {"float" => 1.2} }
    end
  end

  context "string" do
    it_behaves_like "a serializable bson document" do
      let(:raw) { "\x16\x00\x00\x00\x02hello\x00\x06\x00\x00\x00world\x00\x00" }
      let(:doc) { {"hello" => "world"} }
    end
  end

  context "Undefined [deprecated]" do
    let(:raw) { "\v\x00\x00\x00\x06null\x00\x00" }

    it "is deserialized as nil" do
      BSON.deserialize(raw).should eq({ "null" => nil })
    end
  end

  context "object id" do
    it_behaves_like "a serializable bson document" do
      let(:raw) { "\x16\x00\x00\x00\a_id\x00NMf4;9\xB6\x84\a\x00\x00\x01\x00" }
      let(:doc) { {"_id" => BSON::ObjectId.from_string('4e4d66343b39b68407000001')} }
    end
  end

  context "false" do
    it_behaves_like "a serializable bson document" do
      let(:raw) { "\f\x00\x00\x00\btrue\x00\x00\x00" }
      let(:doc) { {"true" => false} }
    end
  end

  context "true" do
    it_behaves_like "a serializable bson document" do
      let(:raw) { "\f\x00\x00\x00\btrue\x00\x01\x00" }
      let(:doc) { {"true" => true} }
    end
  end

  context "utc date time" do
    it_behaves_like "a serializable bson document" do
      let(:raw) { "\x13\x00\x00\x00\tdate\x00\x19\xD6\xA7\xDC1\x01\x00\x00\x00" }
      let(:doc) { {"date" => Time.at(1313667012, 121000).utc } }
    end
  end

  context "nil" do
    it_behaves_like "a serializable bson document" do
      let(:raw) { "\v\x00\x00\x00\nnull\x00\x00" }
      let(:doc) { {"null" => nil} }
    end
  end

  context "regex" do
    context "without flags" do
      it_behaves_like "a serializable bson document" do
        let(:raw) { "\v\x00\x00\x00\vr\x00a\x00\x00\x00" }
        let(:doc) { {"r" => /a/} }
      end
    end
  end

  context "with flags" do
    it_behaves_like "a serializable bson document" do
      let(:raw) { "\x0E\x00\x00\x00\vr\x00a\x00msx\x00\x00" }
      let(:doc) { {"r" => /a/xm} }
    end
  end

  context "db pointer [deprecated]" do
    pending do
      let(:raw) { ",\x00\x00\x00\x03ref\x00\"\x00\x00\x00\x02$ref\x00\x02\x00\x00\x00a\x00\a$id\x00NM\x00\xE2;9\xB6S\xF4\x00\x00\x01\x00\x00" }
      it "is ignored in deserialization" do
        BSON::deserialize(raw).should eq({})
      end
    end
  end

  #context "javascript code" do
  #  it_behaves_like "a serializable bson document" do
  #    let(:raw) { "\x1D\x00\x00\x00\x0Dkeyf\x00\x0E\x00\x00\x00function() {}\x00\x00" }
  #    let(:doc) { {"keyf" => BSON::Code.new("function() {}")} }
  #  end
  #end

  context "symbol" do
    it_behaves_like "a serializable bson document" do
      let(:raw) { "\x0E\x00\x00\x00\x0Es\x00\x02\x00\x00\x00s\x00\x00" }
      let(:doc) { {"s" => :s} }
    end
  end

  context "javascript code with scope" do
    it_behaves_like "a serializable bson document" do
      let(:raw) { "-\x00\x00\x00\x0Fkeyf\x00\"\x00\x00\x00\x0E\x00\x00\x00function() {}\x00\f\x00\x00\x00\x10a\x00\x01\x00\x00\x00\x00\x00" }
      let(:doc) {
        { "keyf" => BSON::Code.new("function() {}", { "a" => 1 }) }
      }
    end
  end

  context "32 bit integer" do
    it_behaves_like "a serializable bson document" do
      let(:raw) { "\f\x00\x00\x00\x10n\x00d\x00\x00\x00\x00" }
      let(:doc) { {"n" => 100} }
    end
  end

  context "timestamp" do
    it_behaves_like "a serializable bson document" do
      let(:raw) { "\x10\x00\x00\x00\x11n\x00e\x00\x00\x00d\x00\x00\x00\x00" }
      let(:doc) { {"n" => BSON::Timestamp.new(100, 101)} }
    end
  end

  #context "min key" do
  #  it_behaves_like "a serializable bson document" do
  #    let(:raw) { "\b\x00\x00\x00\xFFn\x00\x00" }
  #    let(:doc) { {"n" => BSON::MinKey} }
  #  end
  #end

  #context "max key" do
  #  it_behaves_like "a serializable bson document" do
  #    let(:raw) { "\b\x00\x00\x00\x7Fn\x00\x00" }
  #    let(:doc) { {"n" => BSON::MaxKey} }
  #  end
  #end

  context "64 bit integer" do
    it_behaves_like "a serializable bson document" do
      let(:raw) { "\x10\x00\x00\x00\x12n\x00\x00\xE8vH\x17\x00\x00\x00\x00" }
      let(:doc) { {"n" => 100_000_000_000} }
    end

    context "when the number is too large" do
      it "raises a RangeError" do
        lambda { BSON.serialize("n" => 2**64 / 2) }.should \
          raise_exception(RangeError)
      end
    end

    context "when the number is too small" do
      it "raises a RangeError" do
        lambda { BSON.serialize("n" => -2**64 / 2 - 1) }.should \
          raise_exception(RangeError)
      end
    end
  end

  context "embedded document" do
    it_behaves_like "a serializable bson document" do
      let(:raw) { "\x1D\x00\x00\x00\x03embedded\x00\x0E\x00\x00\x00\x02a\x00\x02\x00\x00\x00b\x00\x00\x00" }
      let(:doc) { {"embedded" => {"a" => "b"}} }
    end
  end

  context "embedded array" do
    it_behaves_like "a serializable bson document" do
      let(:raw) { "\x1D\x00\x00\x00\x04embedded\x00\x0E\x00\x00\x00\x020\x00\x02\x00\x00\x00b\x00\x00\x00" }
      let(:doc) { {"embedded" => ["b"]} }
    end
  end

  context "complex document" do
    it_behaves_like "a serializable bson document" do
      let(:raw) { "1\x00\x00\x00\x04BSON\x00&\x00\x00\x00\x020\x00\x08\x00\x00\x00awesome\x00\x011\x00333333\x14@\x102\x00\xc2\x07\x00\x00\x00\x00" }
      let(:doc) { {"BSON" => ["awesome", 5.05, 1986]} }
    end
  end

=begin
  context "binary" do
    context "generic" do
      it_behaves_like "a serializable bson document" do
        let(:raw) { "\x16\x00\x00\x00\x05data\x00\x06\x00\x00\x00\x00binary\x00" }
        let(:doc) { {"data" => BSON::Binary.new("binary", BSON::Binary::SUBTYPE_BINARY) } }
      end
    end

    context "function" do
      it_behaves_like "a serializable bson document" do
        let(:raw) { "\x16\x00\x00\x00\x05data\x00\x06\x00\x00\x00\x01binary\x00" }
        let(:doc) { {"data" => BSON::Binary.new("binary", BSON::Binary::SUBTYPE_FUNCTION) } }
      end
    end

    context "old" do
      it_behaves_like "a serializable bson document" do
        let(:raw) { "\x1A\x00\x00\x00\x05data\x00\n\x00\x00\x00\x02\x06\x00\x00\x00binary\x00" }
        let(:doc) { {"data" => BSON::Binary.new("binary", BSON::Binary::SUBTYPE_BINARY_OLD) } }
      end
    end

    context "uuid(old)" do
      it_behaves_like "a serializable bson document" do
        let(:raw) { "\x16\x00\x00\x00\x05data\x00\x06\x00\x00\x00\x03binary\x00" }
        let(:doc) { {"data" => BSON::Binary.new("binary", BSON::Binary::SUBTYPE_UUID_OLD) } }
      end
    end

    context "uuid" do
      it_behaves_like "a serializable bson document" do
        let(:raw) { "\x16\x00\x00\x00\x05data\x00\x06\x00\x00\x00\x04binary\x00" }
        let(:doc) { {"data" => BSON::Binary.new("binary", BSON::Binary::SUBTYPE_UUID ) } }
      end
    end

    context "md5" do
      it_behaves_like "a serializable bson document" do
        let(:raw) { "\x16\x00\x00\x00\x05data\x00\x06\x00\x00\x00\x05binary\x00" }
        let(:doc) { {"data" => BSON::Binary.new("binary", BSON::Binary::SUBTYPE_MD5 ) } }
      end
    end

    context "user_defined" do
      it_behaves_like "a serializable bson document" do
        let(:raw) { "\x16\x00\x00\x00\x05data\x00\x06\x00\x00\x00\x80binary\x00" }
        let(:doc) { {"data" => BSON::Binary.new("binary", BSON::Binary::SUBTYPE_USER_DEFINED ) } }
      end
    end
  end
=end
  context "utf8 data" do
    it "handles utf-8 keys" do
      doc = { "gültig" => "type" }
      BSON.deserialize(BSON::serialize(doc)).should eq doc
    end

    it "handles utf-8 string values" do
      doc = { "_id" => BSON::ObjectId.new, "type" => "gültig" }
      BSON.deserialize(BSON.serialize(doc)).should eq doc
    end

    it "handles utf-8 keys and values" do
      doc = { "_id" => BSON::ObjectId.new, "gültig" => "gültig" }
      BSON.deserialize(BSON::serialize(doc)).should eq doc
    end

    it "handles utf-8 regexp values" do
      doc = { "_id" => BSON::ObjectId.new, "type" => /^gültig/ }
      BSON.deserialize(BSON::serialize(doc)).should eq doc
    end

    #it "handles utf-8 symbol values" do
    #  doc = { "_id" => BSON::ObjectId.new, "type" => "gültig".to_sym }
    #  BSON.deserialize(BSON::serialize(doc)).should eq doc
    #end

    it "handles utf-8 string values in an array" do
      doc = { "_id" => BSON::ObjectId.new, "type" => ["gültig"] }
      BSON.deserialize(BSON::serialize(doc)).should eq doc
    end

    it "handles utf-8 code values" do
      doc = { "_id" => BSON::ObjectId.new, "code" => BSON::Code.new("// gültig") }
      BSON.deserialize(BSON::serialize(doc)).should eq doc
    end

    it "handles utf-8 code with scope values" do
      doc = { "_id" => BSON::ObjectId.new, "code" => BSON::Code.new("// gültig", {}) }
      BSON.deserialize(BSON::serialize(doc)).should eq doc
    end

    #it "tries to encode non-utf8 data to utf-8" do
    #  string = "gültig"
    #  doc = { "type" => string.encode('iso-8859-1') }

    #  BSON.deserialize(BSON::serialize(doc)).should eq \
    #    Hash["type" => string]
    #end

    #it "handles binary string values of utf-8 content" do
    #  string = "europäischen"
    #  doc = { "type" => string.encode('binary', 'binary') }
    #  BSON.deserialize(StringIO.new(BSON.serialize(doc))).should eq \
    #    Hash["type" => string]
    #end unless RUBY_PLATFORM =~ /java/

    #it "raises an exception for keys with null bytes" do
    #  lambda do
    #    BSON.serialize("key\x00" => "value")
    #  end.should raise_exception(EncodingError)
    #end

    #it "raises an exception for binary string values of non utf-8 content" do
    #  lambda do
    #    BSON.serialize({ "type" => 255.chr })
    #  end.should raise_exception(BSON::InvalidStringEncoding)
    #end unless RUBY_PLATFORM =~ /java/

    #context "with a non-utf8 internal encoding" do
    #  around do |example|
    #    original, Encoding.default_internal = Encoding.default_internal, 'iso-8859-1'
    #    example.run
    #    Encoding.default_internal = original
    #  end

    #  it "returns data in the user's internal encoding" do
    #    string = "R\xE9sum\xE9".force_encoding('iso-8859-1')
    #
    #    doc = { "string" => string }
    #    BSON.deserialize(BSON.serialize(doc)).should eq Hash["string" => string]
    #  end
    #end
  end
end
