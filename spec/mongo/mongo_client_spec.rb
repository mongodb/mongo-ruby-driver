require "spec_helper"

describe Mongo::MongoClient do
  let(:client) do
    described_class.new
  end

  it "connects automatically" do
    client.connected?.should eq(true)
  end

  describe "#initialize" do
    context "defaults" do
      it("defaults to localhost") { client.host.should eq("localhost")}
      it("defaults to port 27017") { client.port.should eq(27017) }
    end
  end
end