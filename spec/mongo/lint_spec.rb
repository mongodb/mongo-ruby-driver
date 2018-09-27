require 'lite_spec_helper'

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
end
