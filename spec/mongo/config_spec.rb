# frozen_string_literal: true
# rubocop:todo all

require "spec_helper"

describe Mongo::Config do

  shared_examples "a config option" do

    before do
      Mongo::Config.reset
    end

    context 'when the value is false' do

      before do
        Mongo.send("#{option}=", false)
      end

      it "is set to false" do
        expect(Mongo.send(option)).to be(false)
      end
    end

    context 'when the value is true' do

      before do
        Mongo.send("#{option}=", true)
      end

      it "is set to true" do
        expect(Mongo.send(option)).to be(true)
      end
    end

    context "when it is not set in the config" do

      it "it is set to its default" do
        expect(Mongo.send(option)).to be(default)
      end
    end
  end


  context 'when setting the validate_update_replace option in the config' do
    let(:option) { :validate_update_replace }
    let(:default) { false }

    it_behaves_like "a config option"
  end

  describe "#options=" do

    context "when an option" do

      before do
        described_class.options = { validate_update_replace: true }
      end

      it "assigns the option correctly" do
        expect(described_class.validate_update_replace).to be true
      end
    end

    context "when provided a non-existent option" do

      it "raises an error" do
        expect {
          described_class.options = { bad_option: true }
        }.to raise_error(Mongo::Error::InvalidConfigOption)
      end
    end
  end
end
