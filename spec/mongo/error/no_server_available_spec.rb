# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::Error::NoServerAvailable do
  describe 'message' do
    let(:selector) do
      Mongo::ServerSelector::Primary.new
    end

    let(:cluster) do
      Mongo::Cluster.new(['127.0.0.1:27017'],
        Mongo::Monitoring.new, monitoring_io: false)
    end

    let(:error) do
      Mongo::Error::NoServerAvailable.new(selector, cluster)
    end

    it 'is correct' do
      expect(error.message).to eq('No primary server is available in cluster: #<Cluster topology=Unknown[127.0.0.1:27017] servers=[#<Server address=127.0.0.1:27017 UNKNOWN NO-MONITORING>]> with timeout=30, LT=0.015')
    end

    context 'when cluster is nil' do
      let(:error) do
        Mongo::Error::NoServerAvailable.new(selector, nil)
      end

      it 'is correct' do
        expect(error.message).to eq('No primary server is available with timeout=30, LT=0.015')
      end
    end
  end
end
