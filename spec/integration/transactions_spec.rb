# frozen_string_literal: true

require "spec_helper"

describe "Transactions" do
  context "when a non transient error is caught and not propagated" do
    let(:collection) { authorized_client[:transactions_spec] }
    before do
      collection.drop
      collection.insert_one(_id: 1)
    end

    it "does not fall into an infinite loop" do
      session = authorized_client.start_session
      session.with_transaction do
        collection.insert_one({ _id: 1 }, session: session)
      rescue Mongo::Error::OperationFailure => e
        if e.code == 11_000
          # Ignore duplicate key error
        else
          raise
        end
      end
    end
  end
end
