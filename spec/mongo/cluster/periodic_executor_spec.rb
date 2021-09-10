# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'

describe Mongo::Cluster::PeriodicExecutor do

  let(:cluster) { double('cluster') }

  let(:executor) do
    described_class.new(cluster)
  end

  describe '#log_warn' do
    it 'works' do
      expect do
        executor.log_warn('test warning')
      end.not_to raise_error
    end
  end
end
