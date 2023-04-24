# frozen_string_literal: true
# rubocop:todo all

require "spec_helper"

describe Mongo::QueryCache::Middleware do

  let :middleware do
    described_class.new(app)
  end

  context "when not touching query cache in the app" do

    let(:app) do
      lambda do |env|
        @enabled = Mongo::QueryCache.enabled?
        [200, env, "app"]
      end
    end

    it "returns success" do
      code, _ = middleware.call({})
      expect(code).to eq(200)
    end

    it "enables the query cache" do
      middleware.call({})
      expect(@enabled).to be true
    end
  end

  context "when querying in the app" do

    before do
      authorized_client['test'].insert_one(test: 1)
    end

    let(:app) do
      lambda do |env|
        authorized_client['test'].find
        [200, env, "app"]
      end
    end

    it "returns success" do
      code, _ = middleware.call({})
      expect(code).to eq(200)
    end

    it "cleans the query cache after it responds" do
      middleware.call({})
      expect(Mongo::QueryCache.send(:cache_table)).to be_empty
    end
  end
end
