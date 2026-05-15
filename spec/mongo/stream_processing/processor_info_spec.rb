# frozen_string_literal: true

require 'lite_spec_helper'

describe Mongo::StreamProcessing::ProcessorInfo do
  describe 'getters exposed on the populated fixture' do
    let(:info) do
      described_class.new(
        'id' => 'proc-1',
        'name' => 'smokeTestProcessor',
        'state' => 'CREATED',
        'pipeline' => [ { '$source' => { 'connectionName' => 'sample_stream_solar' } } ],
        'pipelineVersion' => 2,
        'tier' => 'SP2',
        'streamMetaFieldName' => '_stream_meta',
        'enableAutoScaling' => true,
        'failoverEnabled' => false,
        'activeRegion' => 'us-east-1',
        'workspaceDefaultRegion' => 'us-east-1',
        'modifiedBy' => 'user-1',
        'hasStarted' => false,
        'errorMsg' => '',
        'errorRetryable' => false
      )
    end

    it 'exposes scalar fields' do
      expect(info.id).to eq('proc-1')
      expect(info.name).to eq('smokeTestProcessor')
      expect(info.state).to eq('CREATED')
      expect(info.pipeline_version).to eq(2)
      expect(info.tier).to eq('SP2')
      expect(info.stream_meta_field_name).to eq('_stream_meta')
      expect(info.active_region).to eq('us-east-1')
      expect(info.workspace_default_region).to eq('us-east-1')
      expect(info.modified_by).to eq('user-1')
    end

    it 'returns boolean predicates' do
      expect(info.auto_scaling_enabled?).to be true
      expect(info.failover_enabled?).to be false
      expect(info.started?).to be false
      expect(info.error_retryable?).to be false
    end

    it 'returns the pipeline as an array' do
      expect(info.pipeline).to eq([ { '$source' => { 'connectionName' => 'sample_stream_solar' } } ])
    end

    it 'returns the empty error message' do
      expect(info.error_msg).to eq('')
    end
  end

  describe 'getters when optional fields are missing' do
    let(:info) do
      described_class.new('name' => 'p', 'state' => 'CREATED')
    end

    it 'returns nil or sensible defaults' do
      expect(info.id).to be_nil
      expect(info.pipeline_version).to be_nil
      expect(info.tier).to be_nil
      expect(info.dlq).to be_nil
      expect(info.stream_meta_field_name).to be_nil
      expect(info.active_region).to be_nil
      expect(info.workspace_default_region).to be_nil
      expect(info.last_state_change).to be_nil
      expect(info.last_modified_at).to be_nil
      expect(info.modified_by).to be_nil
      expect(info.error_code).to be_nil
    end

    it 'returns false for boolean predicates' do
      expect(info.auto_scaling_enabled?).to be false
      expect(info.failover_enabled?).to be false
      expect(info.started?).to be false
      expect(info.error_retryable?).to be false
    end

    it 'returns empty string for error_msg' do
      expect(info.error_msg).to eq('')
    end

    it 'returns empty array for pipeline' do
      expect(info.pipeline).to eq([])
    end
  end

  describe 'getters when error fields are set' do
    let(:info) do
      described_class.new(
        'name' => 'p',
        'state' => 'FAILED',
        'errorMsg' => 'something went wrong',
        'errorRetryable' => true,
        'errorCode' => 125
      )
    end

    it 'exposes the error message' do
      expect(info.error_msg).to eq('something went wrong')
    end

    it 'exposes retryability' do
      expect(info.error_retryable?).to be true
    end

    it 'exposes the error code' do
      expect(info.error_code).to eq(125)
    end
  end

  describe '#[]' do
    it 'reads the raw document' do
      info = described_class.new('name' => 'p', 'state' => 'CREATED')
      expect(info[:name]).to eq('p')
      expect(info['state']).to eq('CREATED')
      expect(info[:nope]).to be_nil
    end
  end
end
