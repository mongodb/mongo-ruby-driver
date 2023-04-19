# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Operation::SessionsSupported do
  # https://jira.mongodb.org/browse/RUBY-2224
  require_no_linting

  let(:selector) do
    BSON::Document.new(name: 'test')
  end

  let(:options) do
    {}
  end

  let(:cluster) do
    double('cluster').tap do |cluster|
      allow(cluster).to receive(:single?).and_return(single?)
    end
  end

  let(:operation) do
    Class.new do
      include Mongo::Operation::SessionsSupported
    end.new.tap do |op|
      allow(op).to receive(:read).and_return(read_pref)
      allow(op).to receive(:selector).and_return(selector)
      allow(op).to receive(:options).and_return(options)
    end
  end

  let(:description) do
    double('description').tap do |description|
      allow(description).to receive(:mongos?).and_return(mongos?)
      allow(description).to receive(:standalone?).and_return(standalone?)
    end
  end

  let(:server) do
    double('server').tap do |server|
      allow(server).to receive(:cluster).and_return(cluster)
      # TODO consider adding tests for load-balanced topologies also
      allow(server).to receive(:load_balancer?).and_return(false)
    end
  end

  let(:connection) do
    double('connection').tap do |connection|
      allow(connection).to receive(:server).and_return(server)
      allow(connection).to receive(:description).and_return(description)
    end
  end

  describe '#add_read_preference' do

    let(:read_pref) do
      Mongo::ServerSelector.get(:mode => mode)
    end

    let(:actual) do
      sel = operation.send(:selector).dup
      operation.send(:add_read_preference, sel, connection)
      sel
    end

    let(:expected_read_preference) do
      {mode: mode.to_s.gsub(/_(.)/) { $1.upcase }}
    end

    shared_examples_for 'adds read preference' do

      let(:expected) do
        selector.merge(:$readPreference => expected_read_preference)
      end

      it 'adds read preference' do
        expect(actual).to eq(expected)
      end
    end

    shared_examples_for 'does not modify selector' do

      it 'does not modify selector' do
        expect(actual).to eq(selector)
      end
    end

    shared_examples_for 'does not send read preference' do
      ([nil] + %i(primary primary_preferred secondary secondary_preferred nearest)).each do |_mode|
        active_mode = _mode

        context "when read preference mode is #{active_mode}" do
          let(:mode) { active_mode }

          it_behaves_like 'does not modify selector'
        end
      end
    end

    shared_examples_for 'sends read preference correctly for replica set' do
      context "when read preference mode is primary" do
        let(:mode) { :primary}

        it_behaves_like 'does not modify selector'
      end
      %i(primary_preferred secondary secondary_preferred nearest).each do |_mode|
        active_mode = _mode

        context "when read preference mode is #{active_mode}" do
          let(:mode) { active_mode }

          let(:expected) do
            selector.merge(:$readPreference => expected_read_preference)
          end

          it 'adds read preference' do
            expect(actual).to eq(expected)
          end
        end
      end
    end

    shared_examples_for 'sends user-specified read preference' do
      %i(primary primary_preferred secondary secondary_preferred nearest).each do |_mode|
        active_mode = _mode

        context "when read preference mode is #{active_mode}" do
          let(:mode) { active_mode }

          it_behaves_like 'adds read preference'
        end
      end

      context "when read preference mode is nil" do
        let(:mode) { nil }

        let(:expected_read_preference) do
          {mode: 'primary'}
        end

        it_behaves_like 'adds read preference'
      end
    end

    shared_examples_for 'changes read preference to allow secondary reads' do

      %i(primary_preferred secondary secondary_preferred nearest).each do |_mode|
        active_mode = _mode

        context "when read preference mode is #{active_mode}" do
          let(:mode) { active_mode }

          it_behaves_like 'adds read preference'
        end
      end

      context "when read preference mode is primary" do
        let(:mode) { :primary }

        let(:expected_read_preference) do
          {mode: 'primaryPreferred'}
        end

        it_behaves_like 'adds read preference'
      end

      context "when read preference mode is nil" do
        let(:mode) { nil }

        let(:expected_read_preference) do
          {mode: 'primaryPreferred'}
        end

        it_behaves_like 'adds read preference'
      end
    end

    shared_examples_for 'sends read preference correctly for mongos' do
      %i(primary_preferred secondary nearest).each do |_mode|
        active_mode = _mode

        context "when read preference mode is #{active_mode}" do
          let(:mode) { active_mode }

          it_behaves_like 'adds read preference'
        end
      end

      context 'when read preference mode is primary' do
        let(:mode) { 'primary' }
        it_behaves_like 'does not modify selector'
      end

      context 'when read preference mode is secondary_preferred' do
        let(:mode) { 'secondary_preferred' }

        let(:read_pref) do
          Mongo::ServerSelector.get(mode: mode, tag_sets: tag_sets)
        end

        let(:tag_sets) { nil }

        context 'without tag_sets specified' do
          it_behaves_like 'adds read preference'
        end

        context 'with empty tag_sets' do
          let(:tag_sets) { [] }

          it_behaves_like 'adds read preference'
        end

        context 'with tag_sets specified' do
          let(:tag_sets) { [{ dc: 'ny' }] }

          let(:expected_read_preference) do
            { mode: 'secondaryPreferred', tags: tag_sets }
          end

          it_behaves_like 'adds read preference'
        end
      end
    end

    context 'in single topology' do
      let(:single?) { true }

      context 'when the server is a standalone' do

        let(:standalone?) { true }
        let(:mongos?) { false }

        it_behaves_like 'does not send read preference'
      end

      context 'when the server is a mongos' do

        let(:standalone?) { false }
        let(:mongos?) { true }

        it_behaves_like 'sends read preference correctly for mongos'
      end

      context 'when the server is a replica set member' do

        let(:standalone?) { false }
        let(:mongos?) { false }

        it_behaves_like 'changes read preference to allow secondary reads'
      end
    end

    context 'not in single topology' do
      let(:single?) { false }

      context 'when the server is a standalone' do

        let(:standalone?) { true }
        let(:mongos?) { false }

        it_behaves_like 'does not send read preference'
      end

      context 'when the server is a mongos' do

        let(:standalone?) { false }
        let(:mongos?) { true }

        it_behaves_like 'sends read preference correctly for mongos'

        context 'when read preference mode is secondary_preferred' do
          let(:read_pref) do
            Mongo::ServerSelector.get(
              mode: mode,
              tag_sets: tag_sets,
              hedge: hedge
            )
          end

          let(:mode) { 'secondary_preferred' }
          let(:tag_sets) { nil }
          let(:hedge) { nil }

          context 'when tag_sets and hedge are not specified' do
            it_behaves_like 'adds read preference'
          end

          context 'when tag_sets are specified' do
            let(:tag_sets) { [{ dc: 'ny' }] }

            let(:expected_read_preference) do
              { mode: 'secondaryPreferred', tags: tag_sets }
            end

            it_behaves_like 'adds read preference'
          end

          context 'when hedge is specified' do
            let(:hedge) { { enabled: true } }

            let(:expected_read_preference) do
              { mode: 'secondaryPreferred', hedge: hedge }
            end

            it_behaves_like 'adds read preference'
          end

          context 'when hedge and tag_sets are specified' do
            let(:hedge) { { enabled: true } }
            let(:tag_sets) { [{ dc: 'ny' }] }

            let(:expected_read_preference) do
              { mode: 'secondaryPreferred', tags: tag_sets, hedge: hedge }
            end

            it_behaves_like 'adds read preference'
          end
        end
      end

      context 'when the server is a replica set member' do

        let(:standalone?) { false }
        let(:mongos?) { false }

        it_behaves_like 'sends read preference correctly for replica set'
      end
    end
  end
end
