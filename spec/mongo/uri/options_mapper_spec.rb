# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::URI::OptionsMapper do

  let(:options_mapper) { described_class.new }
  let(:converted) { options_mapper.send(method, name, value) }
  let(:reverted) { options_mapper.send(method, value) }
  let(:name) { "name" }

  describe "#convert_bool" do

    let(:method) { :convert_bool }

    context "when providing false" do
      let(:value) { false }

      it "returns false" do
        expect(converted).to be false
      end
    end

    context "when providing true" do
      let(:value) { true }

      it "returns true" do
        expect(converted).to be true
      end
    end

    context "when providing a true string" do
      let(:value) { "true" }

      it "returns true" do
        expect(converted).to be true
      end
    end

    context "when providing a capital true string" do
      let(:value) { "TRUE" }

      it "returns true" do
        expect(converted).to be true
      end
    end

    context "when providing a false string" do
      let(:value) { "false" }

      it "returns false" do
        expect(converted).to be false
      end
    end

    context "when providing a false string" do
      let(:value) { "FALSE" }

      it "returns false" do
        expect(converted).to be false
      end
    end

    context "when providing a different string" do
      let(:value) { "hello" }

      it "returns nil" do
        expect(converted).to be nil
      end
    end

    context "when providing a different type" do
      let(:value) { :hello }

      it "returns nil" do
        expect(converted).to be nil
      end
    end
  end

  describe "#revert_bool" do

    let(:method) { :revert_bool }

    context "when passing a boolean" do
      let(:value) { true }

      it "returns the boolean" do
        expect(reverted).to eq(value)
      end
    end

    context "when passing nil" do
      let(:value) { nil }

      it "returns nil" do
        expect(reverted).to be nil
      end
    end
  end

  describe "#stringify_bool" do

    let(:method) { :stringify_bool }

    context "when passing a boolean" do
      let(:value) { true }

      it "returns a string" do
        expect(reverted).to eq("true")
      end
    end

    context "when passing false" do
      let(:value) { false }

      it "returns a string" do
        expect(reverted).to eq("false")
      end
    end

    context "when passing nil" do
      let(:value) { nil }

      it "returns nil" do
        expect(reverted).to be nil
      end
    end
  end

  describe "#convert_repeated_bool" do

    let(:method) { :convert_repeated_bool }
    let(:value) { true }

    it "wraps the result in an array" do
      expect(converted).to eq([ true ])
    end
  end

  describe "#revert_repeated_bool" do

    let(:method) { :revert_repeated_bool }

    context "when passing a boolean list" do
      let(:value) { [ true ] }

      it "returns the passed value" do
        expect(reverted).to eq(value)
      end
    end

    context "when passing nil" do
      let(:value) { nil }

      it "returns nil" do
        expect(reverted).to be nil
      end
    end
  end

  describe "#stringify_repeated_bool" do

    let(:method) { :stringify_repeated_bool }

    context "when passing a boolean list" do
      let(:value) { [ true ] }

      it "returns a string" do
        expect(reverted).to eq("true")
      end
    end

    context "when passing a multi boolean list" do
      let(:value) { [ true, false ] }

      it "returns a string" do
        expect(reverted).to eq("true,false")
      end
    end

    context "when passing nil" do
      let(:value) { nil }

      it "returns nil" do
        expect(reverted).to be nil
      end
    end
  end

  describe "#convert_inverse_bool" do

    let(:method) { :convert_inverse_bool }

    context "when providing false" do
      let(:value) { false }

      it "returns false" do
        expect(converted).to be true
      end
    end

    context "when providing true" do
      let(:value) { true }

      it "returns true" do
        expect(converted).to be false
      end
    end

    context "when providing a true string" do
      let(:value) { "true" }

      it "returns true" do
        expect(converted).to be false
      end
    end

    context "when providing a capital true string" do
      let(:value) { "TRUE" }

      it "returns true" do
        expect(converted).to be false
      end
    end

    context "when providing a false string" do
      let(:value) { "false" }

      it "returns false" do
        expect(converted).to be true
      end
    end

    context "when providing a false string" do
      let(:value) { "FALSE" }

      it "returns false" do
        expect(converted).to be true
      end
    end

    context "when providing a different string" do
      let(:value) { "hello" }

      it "returns nil" do
        expect(converted).to be nil
      end
    end

    context "when providing a different type" do
      let(:value) { :hello }

      it "returns nil" do
        expect(converted).to be nil
      end
    end
  end

  describe "#revert_inverse_bool" do

    let(:method) { :revert_inverse_bool }

    context "when passing true" do
      let(:value) { true }

      it "returns false" do
        expect(reverted).to be false
      end
    end

    context "when passing false" do
      let(:value) { false }

      it "returns true" do
        expect(reverted).to be true
      end
    end

    context "when passing nil" do
      let(:value) { nil }

      it "returns nil" do
        expect(reverted).to be nil
      end
    end
  end

  describe "#stringify_inverse_bool" do

    let(:method) { :stringify_inverse_bool }

    context "when passing true" do
      let(:value) { true }

      it "returns false string" do
        expect(reverted).to eq("false")
      end
    end

    context "when passing false" do
      let(:value) { false }

      it "returns true string" do
        expect(reverted).to eq("true")
      end
    end

    context "when passing nil" do
      let(:value) { nil }

      it "returns nil" do
        expect(reverted).to be nil
      end
    end
  end

  describe "#convert_integer" do

    let(:method) { :convert_integer }

    context "when passing an integer" do
      let(:value) { 1 }

      it "returns as an integer" do
        expect(converted).to eq(1)
      end
    end

    context "when passing an integer string" do
      let(:value) { "42" }

      it "returns as an integer" do
        expect(converted).to eq(42)
      end
    end

    context "when passing an invalid string" do
      let(:value) { "hello" }

      it "returns nil" do
        expect(converted).to be nil
      end
    end
  end

  describe "#revert_integer" do

    let(:method) { :revert_integer }

    context "when passing an integer" do
      let(:value) { 1 }

      it "returns the passed value" do
        expect(reverted).to eq(value)
      end
    end

    context "when passing nil" do
      let(:value) { nil }

      it "returns nil" do
        expect(reverted).to be nil
      end
    end
  end

  describe "#stringify_integer" do

    let(:method) { :stringify_integer }

    context "when passing an integer" do
      let(:value) { 1 }

      it "returns the passed value as a string" do
        expect(reverted).to eq("1")
      end
    end

    context "when passing nil" do
      let(:value) { nil }

      it "returns nil" do
        expect(reverted).to be nil
      end
    end
  end

  describe "#convert_ms" do

    let(:method) { :convert_ms }

    context "when passing an integer" do
      let(:value) { 1000 }

      it "returns a float divided by 1000" do
        expect(converted).to eq(1.0)
      end
    end

    context "when passing a negative integer" do
      let(:value) { -1000 }

      it "returns a float divided by 1000" do
        expect(converted).to be nil
      end
    end

    context "when passing an integer string" do
      let(:value) { "1000" }

      it "returns a float divided by 1000" do
        expect(converted).to eq(1.0)
      end
    end

    context "when passing a negative integer string" do
      let(:value) { "-1000" }

      it "returns a float divided by 1000" do
        expect(converted).to be nil
      end
    end

    context "when passing a float string" do
      let(:value) { "1000.5" }

      it "returns a float divided by 1000" do
        expect(converted).to eq(1.0005)
      end
    end

    context "when passing a negative float string" do
      let(:value) { "-1000.5" }

      it "returns a float divided by 1000" do
        expect(converted).to be nil
      end
    end

    context "when passing a float" do
      let(:value) { 1000.5 }

      it "returns a float divided by 1000" do
        expect(converted).to eq(1.0005)
      end
    end

    context "when passing a negative float" do
      let(:value) { -1000.5 }

      it "returns a float divided by 1000" do
        expect(converted).to be nil
      end
    end
  end

  describe "#revert_ms" do

    let(:method) { :revert_ms }

    context "when passing a float" do
      let(:value) { 1.000005 }

      it "returns an integer" do
        expect(reverted).to eq(1000)
      end
    end
  end

  describe "#stringify_ms" do

    let(:method) { :stringify_ms }

    context "when passing a float" do
      let(:value) { 1.000005 }

      it "returns a string" do
        expect(reverted).to eq("1000")
      end
    end
  end

  describe "#convert_symbol" do

    let(:method) { :convert_symbol }

    context "when passing a string" do
      let(:value) { "hello" }

      it "returns a symbol" do
        expect(converted).to eq(:hello)
      end
    end

    context "when passing a symbol" do
      let(:value) { :hello }

      it "returns a symbol" do
        expect(converted).to eq(:hello)
      end
    end
  end

  describe "#revert_symbol" do

    let(:method) { :revert_symbol }

    context "when passing a symbol" do
      let(:value) { :hello }

      it "returns it as a string" do
        expect(reverted).to eq("hello")
      end
    end
  end

  describe "#stringify_symbol" do

    let(:method) { :stringify_symbol }

    context "when passing a symbol" do
      let(:value) { :hello }

      it "returns it as a string" do
        expect(reverted).to eq("hello")
      end
    end
  end

  describe "#convert_array" do

    let(:method) { :convert_array }

    context "when passing a string with no commas" do
      let(:value) { "hello" }

      it "returns one element" do
        expect(converted).to eq([ "hello" ])
      end
    end

    context "when passing a string with commas" do
      let(:value) { "1,2,3" }

      it "returns multiple elements" do
        expect(converted).to eq([ '1', '2', '3' ])
      end
    end
  end

  describe "#revert_array" do

    let(:method) { :revert_array }

    context "when passing one value" do
      let(:value) { [ "hello" ] }

      it "returns the value" do
        expect(reverted).to eq(value)
      end
    end

    context "when passing multiple value" do
      let(:value) { [ "1", "2", "3" ] }

      it "returns the value" do
        expect(reverted).to eq(value)
      end
    end
  end

  describe "#stringify_array" do

    let(:method) { :stringify_array }

    context "when passing one value" do
      let(:value) { [ "hello" ] }

      it "returns a string" do
        expect(reverted).to eq("hello")
      end
    end

    context "when passing multiple value" do
      let(:value) { [ "1", "2", "3" ] }

      it "returns the joined string" do
        expect(reverted).to eq("1,2,3")
      end
    end
  end

  describe "#convert_auth_mech" do

    let(:method) { :convert_auth_mech }

    context "when passing GSSAPI" do
      let(:value) { "GSSAPI" }

      it "returns it as a symbol" do
        expect(converted).to eq(:gssapi)
      end
    end

    context "when passing MONGODB-AWS" do
      let(:value) { "MONGODB-AWS" }

      it "returns it as a symbol" do
        expect(converted).to eq(:aws)
      end
    end

    context "when passing MONGODB-CR" do
      let(:value) { "MONGODB-CR" }

      it "returns it as a symbol" do
        expect(converted).to eq(:mongodb_cr)
      end
    end

    context "when passing MONGODB-X509" do
      let(:value) { "MONGODB-X509" }

      it "returns it as a symbol" do
        expect(converted).to eq(:mongodb_x509)
      end
    end

    context "when passing PLAIN" do
      let(:value) { "PLAIN" }

      it "returns it as a symbol" do
        expect(converted).to eq(:plain)
      end
    end

    context "when passing SCRAM-SHA-1" do
      let(:value) { "SCRAM-SHA-1" }

      it "returns it as a symbol" do
        expect(converted).to eq(:scram)
      end
    end

    context "when passing SCRAM-SHA-256" do
      let(:value) { "SCRAM-SHA-256" }

      it "returns it as a symbol" do
        expect(converted).to eq(:scram256)
      end
    end

    context "when passing a bogus value" do
      let(:value) { "hello" }

      it "returns the value" do
        expect(converted).to eq("hello")
      end

      it "warns" do
        expect(options_mapper).to receive(:log_warn).once
        converted
      end
    end
  end

  describe "#revert_auth_mech" do

    let(:method) { :revert_auth_mech }

    context "when passing GSSAPI" do
      let(:value) { :gssapi }

      it "returns it as a string" do
        expect(reverted).to eq("GSSAPI")
      end
    end

    context "when passing MONGODB-AWS" do
      let(:value) { :aws }

      it "returns it as a string" do
        expect(reverted).to eq("MONGODB-AWS")
      end
    end

    context "when passing MONGODB-CR" do
      let(:value) { :mongodb_cr }

      it "returns it as a string" do
        expect(reverted).to eq("MONGODB-CR")
      end
    end

    context "when passing MONGODB-X509" do
      let(:value) { :mongodb_x509 }

      it "returns it as a string" do
        expect(reverted).to eq("MONGODB-X509")
      end
    end

    context "when passing PLAIN" do
      let(:value) { :plain }

      it "returns it as a string" do
        expect(reverted).to eq("PLAIN")
      end
    end

    context "when passing SCRAM-SHA-1" do
      let(:value) { :scram }

      it "returns it as a string" do
        expect(reverted).to eq("SCRAM-SHA-1")
      end
    end

    context "when passing SCRAM-SHA-256" do
      let(:value) { :scram256 }

      it "returns it as a string" do
        expect(reverted).to eq("SCRAM-SHA-256")
      end
    end

    context "when passing a bogus value" do
      let(:value) { "hello" }

      it "raises an error" do
        expect do
          reverted
        end.to raise_error(ArgumentError, "Unknown auth mechanism hello")
      end
    end

    context "when passing nil" do
      let(:value) { nil }

      it "raises an error" do
        expect do
          reverted
        end.to raise_error(ArgumentError, "Unknown auth mechanism #{nil}")
      end
    end
  end

  describe "#stringify_auth_mech" do

    let(:method) { :stringify_auth_mech }

    context "when passing GSSAPI" do
      let(:value) { :gssapi }

      it "returns it as a string" do
        expect(reverted).to eq("GSSAPI")
      end
    end

    context "when passing MONGODB-AWS" do
      let(:value) { :aws }

      it "returns it as a string" do
        expect(reverted).to eq("MONGODB-AWS")
      end
    end

    context "when passing MONGODB-CR" do
      let(:value) { :mongodb_cr }

      it "returns it as a string" do
        expect(reverted).to eq("MONGODB-CR")
      end
    end

    context "when passing MONGODB-X509" do
      let(:value) { :mongodb_x509 }

      it "returns it as a string" do
        expect(reverted).to eq("MONGODB-X509")
      end
    end

    context "when passing PLAIN" do
      let(:value) { :plain }

      it "returns it as a string" do
        expect(reverted).to eq("PLAIN")
      end
    end

    context "when passing SCRAM-SHA-1" do
      let(:value) { :scram }

      it "returns it as a string" do
        expect(reverted).to eq("SCRAM-SHA-1")
      end
    end

    context "when passing SCRAM-SHA-256" do
      let(:value) { :scram256 }

      it "returns it as a string" do
        expect(reverted).to eq("SCRAM-SHA-256")
      end
    end

    context "when passing a bogus value" do
      let(:value) { "hello" }

      it "returns nil" do
        expect(reverted).to be nil
      end
    end

    context "when passing nil" do
      let(:value) { nil }

      it "returns nil" do
        expect(reverted).to be nil
      end
    end
  end

  describe "#convert_auth_mech_props" do

    let(:method) { :convert_auth_mech_props }

    context "when including one item" do
      let(:value) { "key:value" }

      it "returns a one element hash" do
        expect(converted).to eq(key: "value")
      end
    end

    context "when including multiple items" do
      let(:value) { "k1:v1,k2:v2" }

      it "returns a multiple element hash" do
        expect(converted).to eq(k1: "v1", k2: "v2")
      end
    end

    context "when including items without a colon" do
      let(:value) { "k1:v1,k2,v2" }

      it "drops those items" do
        expect(converted).to eq(k1: "v1")
      end

      it "warns" do
        expect(options_mapper).to receive(:log_warn).twice
        converted
      end
    end

    context "when giving the empty string" do
      let(:value) { "" }

      it "returns nil" do
        expect(converted).to be nil
      end
    end

    context "when giving no valid options" do
      let(:value) { "k1,k2" }

      it "returns nil" do
        expect(converted).to be nil
      end
    end

    context "when passing CANONICALIZE_HOST_NAME" do

      context "when passing true" do
        let(:value) { "CANONICALIZE_HOST_NAME:true" }

        it "returns true as a boolean" do
          expect(converted).to eq(CANONICALIZE_HOST_NAME: true)
        end
      end

      context "when passing uppercase true" do
        let(:value) { "CANONICALIZE_HOST_NAME:TRUE" }

        it "returns true as a boolean" do
          expect(converted).to eq(CANONICALIZE_HOST_NAME: true)
        end
      end

      context "when passing false" do
        let(:value) { "CANONICALIZE_HOST_NAME:false" }

        it "returns false as a boolean" do
          expect(converted).to eq(CANONICALIZE_HOST_NAME: false)
        end
      end

      context "when passing bogus" do
        let(:value) { "CANONICALIZE_HOST_NAME:bogus" }

        it "returns false as a boolean" do
          expect(converted).to eq(CANONICALIZE_HOST_NAME: false)
        end
      end
    end
  end

  describe "#revert_auth_mech_props" do

    let(:method) { :revert_auth_mech_props }

    context "when including one item" do
      let(:value) { { key: "value" } }

      it "returns a one element hash" do
        expect(reverted).to eq(value)
      end
    end

    context "when including multiple items" do
      let(:value) { { k1: "v1", k2: "v2" } }

      it "returns a multiple element hash" do
        expect(reverted).to eq(value)
      end
    end

    context "when passing nil" do
      let(:value) { nil }

      it "returns nil" do
        expect(reverted).to be nil
      end
    end
  end

  describe "#stringify_auth_mech_props" do

    let(:method) { :stringify_auth_mech_props }

    context "when including one item" do
      let(:value) { { key: "value" } }

      it "returns a string" do
        expect(reverted).to eq("key:value")
      end
    end

    context "when including multiple items" do
      let(:value) { { k1: "v1", k2: "v2" } }

      it "returns a string" do
        expect(reverted).to eq("k1:v1,k2:v2")
      end
    end

    context "when passing nil" do
      let(:value) { nil }

      it "returns nil" do
        expect(reverted).to be nil
      end
    end
  end

  describe "#convert_max_staleness" do

    let(:method) { :convert_max_staleness }

    context "when passing a string" do

      context "when passing a positive integer" do
        let(:value) { "100" }

        it "returns an integer" do
          expect(converted).to eq(100)
        end
      end

      context "when passing a negative integer" do
        let(:value) { "-100" }

        it "returns an integer" do
          expect(converted).to be nil
        end

        it "warns" do
          expect(options_mapper).to receive(:log_warn).once
          converted
        end
      end

      context "when passing a bogus value" do
        let(:value) { "hello" }

        it "returns an integer" do
          expect(converted).to be nil
        end
      end
    end

    context "when passing an integer" do

      context "when passing a positive integer" do
        let(:value) { 100 }

        it "returns an integer" do
          expect(converted).to eq(100)
        end
      end

      context "when passing a negative integer" do
        let(:value) { -100 }

        it "returns an integer" do
          expect(converted).to be nil
        end

        it "warns" do
          expect(options_mapper).to receive(:log_warn).once
          converted
        end
      end

      context "when passing negative 1" do
        let(:value) { -1 }

        it "returns an integer" do
          expect(converted).to be nil
        end

        it "doesn't warn" do
          expect(options_mapper).to receive(:log_warn).never
          converted
        end
      end

      context "when passing 0" do
        let(:value) { 0 }

        it "returns 0" do
          expect(converted).to eq(0)
        end

        it "doesn't warn" do
          expect(options_mapper).to receive(:log_warn).never
          converted
        end
      end

      context "when passing a number less than 90" do
        let(:value) { 50 }

        it "returns nil" do
          expect(converted).to be nil
        end
      end
    end

    context "when passing a bogus value" do
      let(:value) { :hello }

      it "returns nil" do
        expect(converted).to be nil
      end
    end
  end

  describe "#revert_max_staleness" do

    let(:method) { :revert_max_staleness }

    context "when passing an integer" do
      let(:value) { 1 }

      it "returns the integer" do
        expect(reverted).to eq(1)
      end
    end

    context "when passing nil" do
      let(:value) { nil }

      it "returns nil" do
        expect(reverted).to be nil
      end
    end
  end

  describe "#stringify_max_staleness" do

    let(:method) { :stringify_max_staleness }

    context "when passing an integer" do
      let(:value) { 1 }

      it "returns the integer string" do
        expect(reverted).to eq('1')
      end
    end

    context "when passing nil" do
      let(:value) { nil }

      it "returns nil" do
        expect(reverted).to be nil
      end
    end
  end

  describe "#convert_read_mode" do

    let(:method) { :convert_read_mode }

    context "when passing primary" do
      let(:value) { "primary" }

      it "returns it as a symbol" do
        expect(converted).to eq(:primary)
      end
    end

    context "when passing primarypreferred" do
      let(:value) { "primarypreferred" }

      it "returns it as a symbol" do
        expect(converted).to eq(:primary_preferred)
      end
    end

    context "when passing secondary" do
      let(:value) { "secondary" }

      it "returns it as a symbol" do
        expect(converted).to eq(:secondary)
      end
    end

    context "when passing secondarypreferred" do
      let(:value) { "secondarypreferred" }

      it "returns it as a symbol" do
        expect(converted).to eq(:secondary_preferred)
      end
    end

    context "when passing nearest" do
      let(:value) { "nearest" }

      it "returns it as a symbol" do
        expect(converted).to eq(:nearest)
      end
    end

    context "when passing capitalized primary" do
      let(:value) { "Primary" }

      it "returns it as a symbol" do
        expect(converted).to eq(:primary)
      end
    end

    context "when passing a bogus string" do
      let(:value) { "hello" }

      it "returns the string" do
        expect(converted).to eq(value)
      end
    end
  end

  describe "#revert_read_mode" do

    let(:method) { :revert_read_mode }

    context "when passing primary" do
      let(:value) { :primary }

      it "returns it as a string" do
        expect(reverted).to eq("primary")
      end
    end

    context "when passing primarypreferred" do
      let(:value) { :primary_preferred }

      it "returns it as a string" do
        expect(reverted).to eq("primaryPreferred")
      end
    end

    context "when passing secondary" do
      let(:value) { :secondary }

      it "returns it as a string" do
        expect(reverted).to eq("secondary")
      end
    end

    context "when passing secondarypreferred" do
      let(:value) { :secondary_preferred }

      it "returns it as a string" do
        expect(reverted).to eq("secondaryPreferred")
      end
    end

    context "when passing nearest" do
      let(:value) { :nearest }

      it "returns it as a string" do
        expect(reverted).to eq("nearest")
      end
    end

    context "when passing a bogus string" do
      let(:value) { "hello" }

      it "returns the string" do
        expect(reverted).to eq("hello")
      end
    end
  end

  describe "#stringify_read_mode" do

    let(:method) { :stringify_read_mode }

    context "when passing primary" do
      let(:value) { :primary }

      it "returns it as a string" do
        expect(reverted).to eq("primary")
      end
    end

    context "when passing primarypreferred" do
      let(:value) { :primary_preferred }

      it "returns it as a string" do
        expect(reverted).to eq("primaryPreferred")
      end
    end

    context "when passing secondary" do
      let(:value) { :secondary }

      it "returns it as a string" do
        expect(reverted).to eq("secondary")
      end
    end

    context "when passing secondarypreferred" do
      let(:value) { :secondary_preferred }

      it "returns it as a string" do
        expect(reverted).to eq("secondaryPreferred")
      end
    end

    context "when passing nearest" do
      let(:value) { :nearest }

      it "returns it as a string" do
        expect(reverted).to eq("nearest")
      end
    end

    context "when passing a bogus string" do
      let(:value) { "hello" }

      it "returns the string" do
        expect(reverted).to eq("hello")
      end
    end
  end

  describe "#convert_read_tags" do

    let(:method) { :convert_read_tags }

    context "when including one item" do
      let(:value) { "key:value" }

      it "returns a one element hash" do
        expect(converted).to eq([{ key: "value" }])
      end
    end

    context "when including multiple items" do
      let(:value) { "k1:v1,k2:v2" }

      it "returns a multiple element hash" do
        expect(converted).to eq([{ k1: "v1", k2: "v2" }])
      end
    end

    context "when including items without a colon" do
      let(:value) { "k1:v1,k2,v2" }

      it "drops those items" do
        expect(converted).to eq([{ k1: "v1" }])
      end

      it "warns" do
        expect(options_mapper).to receive(:log_warn).twice
        converted
      end
    end

    context "when giving the empty string" do
      let(:value) { "" }

      it "returns nil" do
        expect(converted).to be nil
      end
    end

    context "when giving no valid options" do
      let(:value) { "k1,k2" }

      it "returns nil" do
        expect(converted).to be nil
      end
    end
  end

  describe "#revert_read_tags" do

    let(:method) { :revert_read_tags }

    context "when including one item" do
      let(:value) { [ { key: "value" } ] }

      it "returns the passed value" do
        expect(reverted).to eq(value)
      end
    end

    context "when including multiple items" do
      let(:value) { [ { k1: "v1", k2: "v2" } ] }

      it "returns the passed value" do
        expect(reverted).to eq(value)
      end
    end

    context "when including multiple hashes" do
      let(:value) { [ { k1: "v1", k2: "v2" }, { k3: "v3", k4: "v4" } ] }

      it "returns the passed value" do
        expect(reverted).to eq(value)
      end
    end

    context "when passing nil" do
      let(:value) { nil }

      it "returns nil" do
        expect(reverted).to be nil
      end
    end
  end

  describe "#stringify_read_tags" do

    let(:method) { :stringify_read_tags }

    context "when including one item" do
      let(:value) { [ { key: "value" } ] }

      it "returns a one element string list" do
        expect(reverted).to eq([ "key:value" ])
      end
    end

    context "when including multiple items" do
      let(:value) { [ { k1: "v1", k2: "v2" } ] }

      it "returns a one element string list" do
        expect(reverted).to eq([ "k1:v1,k2:v2" ])
      end
    end

    context "when including multiple hashes" do
      let(:value) { [ { k1: "v1", k2: "v2" }, { k3: "v3", k4: "v4" } ] }

      it "returns a multiple element string list" do
        expect(reverted).to eq([ "k1:v1,k2:v2", "k3:v3,k4:v4" ])
      end
    end

    context "when passing nil" do
      let(:value) { nil }

      it "returns nil" do
        expect(reverted).to be nil
      end
    end
  end

  describe "#convert_w" do

    let(:method) { :convert_w }

    context "when passing majority" do
      let(:value) { 'majority' }

      it "returns it as a symbol" do
        expect(converted).to eq(:majority)
      end
    end

    context "when passing an integer string" do
      let(:value) { '42' }

      it "returns it as an integer" do
        expect(converted).to eq(42)
      end
    end

    context "when passing a bogus string" do
      let(:value) { 'hello' }

      it "returns the string" do
        expect(converted).to eq(value)
      end
    end
  end

  describe "#revert_w" do

    let(:method) { :revert_w }

    context "when passing an integer" do
      let(:value) { 1 }

      it "returns an integer" do
        expect(reverted).to eq(1)
      end
    end

    context "when passing a symbol" do
      let(:value) { :majority }

      it "returns a string" do
        expect(reverted).to eq("majority")
      end
    end

    context "when passing a string" do
      let(:value) { "hello" }

      it "returns a string" do
        expect(reverted).to eq(value)
      end
    end
  end

  describe "#stringify_w" do

    let(:method) { :stringify_w }

    context "when passing an integer" do
      let(:value) { 1 }

      it "returns a string" do
        expect(reverted).to eq('1')
      end
    end

    context "when passing a symbol" do
      let(:value) { :majority }

      it "returns a string" do
        expect(reverted).to eq("majority")
      end
    end

    context "when passing a string" do
      let(:value) { "hello" }

      it "returns a string" do
        expect(reverted).to eq(value)
      end
    end
  end

  describe "#convert_zlib_compression_level" do

    let(:method) { :convert_zlib_compression_level }

    context "when passing an integer string" do
      let(:value) { "1" }

      it "returns it as an integer" do
        expect(converted).to eq(1)
      end
    end

    context "when passing a negative integer string" do
      let(:value) { "-1" }

      it "returns it as an integer" do
        expect(converted).to eq(-1)
      end
    end

    context "when passing a bogus string" do
      let(:value) { "hello" }

      it "returns nil" do
        expect(converted).to be nil
      end
    end

    context "when passing an integer" do
      let(:value) { 1 }

      it "returns the integer" do
        expect(converted).to eq(value)
      end
    end

    context "when passing a negative integer" do
      let(:value) { -1 }

      it "returns the integer" do
        expect(converted).to eq(value)
      end
    end

    context "when passing a out of range integer" do
      let(:value) { 10 }

      it "returns nil" do
        expect(converted).to be nil
      end

      it "warns" do
        expect(options_mapper).to receive(:log_warn).once
        converted
      end
    end

    context "when passing a out of range negative integer" do
      let(:value) { -2 }

      it "returns nil" do
        expect(converted).to be nil
      end

      it "warns" do
        expect(options_mapper).to receive(:log_warn).once
        converted
      end
    end
  end

  describe "#revert_zlib_compression_level" do

    let(:method) { :revert_zlib_compression_level }

    context "when passing an integer" do
      let(:value) { 1 }

      it "returns an integer" do
        expect(reverted).to eq(1)
      end
    end

    context "when passing nil" do
      let(:value) { nil }

      it "returns nil" do
        expect(reverted).to be nil
      end
    end
  end

  describe "#stringify_zlib_compression_level" do

    let(:method) { :stringify_zlib_compression_level }

    context "when passing an integer" do
      let(:value) { 1 }

      it "returns a string" do
        expect(reverted).to eq('1')
      end
    end

    context "when passing nil" do
      let(:value) { nil }

      it "returns nil" do
        expect(reverted).to be nil
      end
    end
  end
end
