# frozen_string_literal: true
# rubocop:todo all

require "spec_helper"

describe Mongo::Config::Options do

  let(:config) do
    Mongo::Config
  end

  describe "#defaults" do

    it "returns the default options" do
      expect(config.defaults).to_not be_empty
    end
  end

  describe "#option" do

    context "when a default is provided" do

      after do
        config.reset
      end

      it "defines a getter" do
        expect(config.validate_update_replace).to be false
      end

      it "defines a setter" do
        expect(config.validate_update_replace = true).to be true
        expect(config.validate_update_replace).to be true
      end

      it "defines a presence check" do
        expect(config.validate_update_replace?).to be false
      end
    end

    context 'when option is not a boolean' do
      before do
        config.validate_update_replace = 'foo'
      end

      after do
        config.reset
      end

      context 'presence check' do
        it 'is a boolean' do
          expect(config.validate_update_replace?).to be true
        end
      end
    end
  end

  describe "#reset" do

    before do
      config.validate_update_replace = true
      config.reset
    end

    it "resets the settings to the defaults" do
      expect(config.validate_update_replace).to be false
    end
  end

  describe "#settings" do

    it "returns the settings" do
      expect(config.settings).to_not be_empty
    end
  end
end
