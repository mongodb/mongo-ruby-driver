require 'spec_helper'

describe Mongo::Session::ServerSession do

  describe '#initialize' do

    it 'sets the last use variable to the current time' do
      expect(described_class.new.last_use).to be_within(0.2).of(Time.now)
    end

    it 'sets a UUID as the session id' do
      expect(described_class.new.session_id).to be_a(BSON::Document)
      expect(described_class.new.session_id[:id]).to be_a(BSON::Binary)
    end
  end
end
