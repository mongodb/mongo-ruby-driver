# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'

describe 'Snapshot Query Examples' do
  require_topology :replica_set, :sharded
  min_server_fcv '5.0'

  let(:uri_string) do
    "mongodb://#{SpecConfig.instance.addresses.join(',')}"
  end

  context "Snapshot Query Example 1" do
    before do
      client = Mongo::Client.new(uri_string, database: "pets")
      client['cats'].delete_many({})
      client['dogs'].delete_many({})

      client['cats'].insert_one({
        name: "Whiskers",
        color: "white",
        age: 10,
        adoptable: true
      })

      client['dogs'].insert_one({
        name: "Pebbles",
        color: "Brown",
        age: 10,
        adoptable: true
      })
    end

    it "returns a snapshot of the data" do

      adoptablePetsCount = 0

      # Start Snapshot Query Example 1

      client = Mongo::Client.new(uri_string, database: "pets")

      client.start_session(snapshot: true) do |session|
        adoptablePetsCount = client['cats'].aggregate([
          { "$match": { "adoptable": true } },
          { "$count": "adoptableCatsCount" }
        ], session: session).first["adoptableCatsCount"]

        adoptablePetsCount += client['dogs'].aggregate([
          { "$match": { "adoptable": true } },
          { "$count": "adoptableDogsCount" }
        ], session: session).first["adoptableDogsCount"]

        puts adoptablePetsCount
      end

      # End Snapshot Query Example 1

      expect(adoptablePetsCount).to eq 2
    end
  end

  context "Snapshot Query Example 2" do
    before do
      client = Mongo::Client.new(uri_string, database: "retail")
      client['sales'].delete_many({})

      client['sales'].insert_one({
        shoeType: "boot",
        price: 30,
        saleDate: Time.now
     })
    end

    it "returns a snapshot of the data" do

      total = 0

      # Start Snapshot Query Example 2

      client = Mongo::Client.new(uri_string, database: "retail")

      client.start_session(snapshot: true) do |session|
        total = client['sales'].aggregate([
          {
             "$match": {
                "$expr": {
                   "$gt": [
                      "$saleDate",
                      {
                         "$dateSubtract": {
                            startDate: "$$NOW",
                            unit: "day",
                            amount: 1
                         }
                      }
                   ]
                 }
              }
          },
          { "$count": "totalDailySales" }
        ], session: session).first["totalDailySales"]
      end

      # End Snapshot Query Example 2

      expect(total).to eq 1
    end
  end
end
