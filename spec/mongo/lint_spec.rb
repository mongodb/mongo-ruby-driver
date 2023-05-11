# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Lint do

  before(:all) do
    # Since we are installing an expectation on ENV, close any open clients
    # which may have background threads reading ENV
    ClientRegistry.instance.close_all_clients
  end

  before do
    expect(ENV).to receive(:[]).with('MONGO_RUBY_DRIVER_LINT').at_least(:once).and_return('1')
  end

  describe '.validate_underscore_read_preference' do
    %w(primary primary_preferred secondary secondary_preferred nearest).each do |mode|
      it "accepts #{mode} as string" do
        expect do
          described_class.validate_underscore_read_preference(mode: mode)
        end.to_not raise_error
      end

      it "accepts #{mode} with string mode key" do
        expect do
          described_class.validate_underscore_read_preference('mode' => mode)
        end.to_not raise_error
      end

      it "accepts #{mode} as symbol" do
        expect do
          described_class.validate_underscore_read_preference(mode: mode.to_sym)
        end.to_not raise_error
      end
    end

    %w(primaryPreferred secondaryPreferred).each do |mode|
      it "rejects #{mode} as string" do
        expect do
          described_class.validate_underscore_read_preference(mode: mode)
        end.to raise_error(Mongo::Error::LintError)
      end

      it "rejects #{mode} with string mode key" do
        expect do
          described_class.validate_underscore_read_preference('mode' => mode)
        end.to raise_error(Mongo::Error::LintError)
      end

      it "rejects #{mode} as symbol" do
        expect do
          described_class.validate_underscore_read_preference(mode: mode.to_sym)
        end.to raise_error(Mongo::Error::LintError)
      end
    end
  end

  describe '.validate_underscore_read_preference_mode' do
    %w(primary primary_preferred secondary secondary_preferred nearest).each do |mode|
      it "accepts #{mode} as string" do
        expect do
          described_class.validate_underscore_read_preference_mode(mode)
        end.to_not raise_error
      end

      it "accepts #{mode} as symbol" do
        expect do
          described_class.validate_underscore_read_preference_mode(mode.to_sym)
        end.to_not raise_error
      end
    end

    %w(primaryPreferred secondaryPreferred).each do |mode|
      it "rejects #{mode} as string" do
        expect do
          described_class.validate_underscore_read_preference_mode(mode)
        end.to raise_error(Mongo::Error::LintError)
      end

      it "rejects #{mode} as symbol" do
        expect do
          described_class.validate_underscore_read_preference_mode(mode.to_sym)
        end.to raise_error(Mongo::Error::LintError)
      end
    end
  end

  describe '.validate_camel_case_read_preference' do
    %w(primary primaryPreferred secondary secondaryPreferred nearest).each do |mode|
      it "accepts #{mode} as string" do
        expect do
          described_class.validate_camel_case_read_preference(mode: mode)
        end.to_not raise_error
      end

      it "accepts #{mode} with string mode key" do
        expect do
          described_class.validate_camel_case_read_preference('mode' => mode)
        end.to_not raise_error
      end

      it "accepts #{mode} as symbol" do
        expect do
          described_class.validate_camel_case_read_preference(mode: mode.to_sym)
        end.to_not raise_error
      end
    end

    %w(primary_preferred secondary_preferred).each do |mode|
      it "rejects #{mode} as string" do
        expect do
          described_class.validate_camel_case_read_preference(mode: mode)
        end.to raise_error(Mongo::Error::LintError)
      end

      it "rejects #{mode} with string mode key" do
        expect do
          described_class.validate_camel_case_read_preference('mode' => mode)
        end.to raise_error(Mongo::Error::LintError)
      end

      it "rejects #{mode} as symbol" do
        expect do
          described_class.validate_camel_case_read_preference(mode: mode.to_sym)
        end.to raise_error(Mongo::Error::LintError)
      end
    end
  end

  describe '.validate_camel_case_read_preference_mode' do
    %w(primary primaryPreferred secondary secondaryPreferred nearest).each do |mode|
      it "accepts #{mode} as string" do
        expect do
          described_class.validate_camel_case_read_preference_mode(mode)
        end.to_not raise_error
      end

      it "accepts #{mode} as symbol" do
        expect do
          described_class.validate_camel_case_read_preference_mode(mode.to_sym)
        end.to_not raise_error
      end
    end

    %w(primary_preferred secondary_preferred).each do |mode|
      it "rejects #{mode} as string" do
        expect do
          described_class.validate_camel_case_read_preference_mode(mode)
        end.to raise_error(Mongo::Error::LintError)
      end

      it "rejects #{mode} as symbol" do
        expect do
          described_class.validate_camel_case_read_preference_mode(mode.to_sym)
        end.to raise_error(Mongo::Error::LintError)
      end
    end
  end

  describe '.validate_read_concern_option' do
    it 'accepts nil' do
      expect do
        described_class.validate_read_concern_option(nil)
      end.to_not raise_error
    end

    it 'accepts empty hash' do
      expect do
        described_class.validate_read_concern_option({})
      end.to_not raise_error
    end

    it "rejects an object which is not a hash" do
      expect do
        described_class.validate_read_concern_option(:local)
      end.to raise_error(Mongo::Error::LintError)
    end

    [:local, :majority, :snapshot].each do |level|
      it "accepts :#{level}" do
        expect do
          described_class.validate_read_concern_option({level: level})
        end.to_not raise_error
      end

      it "rejects #{level} as string" do
        expect do
          described_class.validate_read_concern_option({level: level.to_s})
        end.to raise_error(Mongo::Error::LintError)
      end
    end

    it "rejects a bogus level" do
      expect do
        described_class.validate_read_concern_option({level: :bogus})
      end.to raise_error(Mongo::Error::LintError)
    end

    it "rejects level given as a string key" do
      expect do
        described_class.validate_read_concern_option({'level' => :snapshot})
      end.to raise_error(Mongo::Error::LintError)
    end

    it "rejects a bogus key as symbol" do
      expect do
        described_class.validate_read_concern_option({foo: 'bar'})
      end.to raise_error(Mongo::Error::LintError)
    end

    it "rejects a bogus key as string" do
      expect do
        described_class.validate_read_concern_option({'foo' => 'bar'})
      end.to raise_error(Mongo::Error::LintError)
    end

    %w(afterClusterTime after_cluster_time).each do |key|
      [:to_s, :to_sym].each do |conv|
        key = key.send(conv)

        it "rejects #{key.inspect}" do
          expect do
            described_class.validate_read_concern_option({key => 123})
          end.to raise_error(Mongo::Error::LintError)
        end
      end
    end
  end
end
