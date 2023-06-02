# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Snapshot Query Examples' do
  require_topology :replica_set, :sharded
  require_no_auth
  require_no_tls

  min_server_fcv '5.0'

  let(:uri_string) do
    "mongodb://#{SpecConfig.instance.addresses.join(',')}/?w=majority"
  end

  context "Snapshot Query Example 1" do
    before do
      client = authorized_client.use('pets')
      client['cats', write_concern: { w: :majority }].delete_many
      client['dogs', write_concern: { w: :majority }].delete_many

      client['cats', write_concern: { w: :majority }].insert_one(
        name: "Whiskers",
        color: "white",
        age: 10,
        adoptable: true
      )

      client['dogs', write_concern: { w: :majority }].insert_one(
        name: "Pebbles",
        color: "Brown",
        age: 10,
        adoptable: true
      )
      if ClusterConfig.instance.topology == :sharded
        run_mongos_distincts "pets", "cats"
      else
        wait_for_snapshot(db: 'pets', collection: 'cats')
        wait_for_snapshot(db: 'pets', collection: 'dogs')
      end
    end

    it "returns a snapshot of the data" do

      adoptable_pets_count = 0

      # Start Snapshot Query Example 1

      client = Mongo::Client.new(uri_string, database: "pets")

      client.start_session(snapshot: true) do |session|
        adoptable_pets_count = client['cats'].aggregate([
          { "$match": { "adoptable": true } },
          { "$count": "adoptable_cats_count" }
        ], session: session).first["adoptable_cats_count"]

        adoptable_pets_count += client['dogs'].aggregate([
          { "$match": { "adoptable": true } },
          { "$count": "adoptable_dogs_count" }
        ], session: session).first["adoptable_dogs_count"]

        puts adoptable_pets_count
      end

      # End Snapshot Query Example 1

      expect(adoptable_pets_count).to eq 2
      client.close
    end
  end

  context "Snapshot Query Example 2" do
    retry_test

    before do
      client = authorized_client.use('retail')
      client['sales', write_concern: { w: :majority }].delete_many

      client['sales', write_concern: { w: :majority }].insert_one(
        shoeType: "boot",
        price: 30,
        saleDate: Time.now
      )

      if ClusterConfig.instance.topology == :sharded
        run_mongos_distincts "retail", "sales"
      else
        wait_for_snapshot(db: 'retail', collection: 'sales')
      end
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
          { "$count": "total_daily_sales" }
        ], session: session).first["total_daily_sales"]
      end

      # End Snapshot Query Example 2

      expect(total).to eq 1
      client.close
    end
  end
end
