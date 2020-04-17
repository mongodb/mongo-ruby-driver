require 'spec_helper'

describe Mongo::Operation::SessionsSupported do
  # https://jira.mongodb.org/browse/RUBY-2224
  skip_if_linting

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

  let(:server) do
    double('server').tap do |server|
      allow(server).to receive(:cluster).and_return(cluster)
      allow(server).to receive(:mongos?).and_return(mongos?)
      allow(server).to receive(:standalone?).and_return(standalone?)
    end
  end

  let(:connection) do
    double('connection').tap do |connection|
      allow(connection).to receive(:server).and_return(server)
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

        it_behaves_like 'changes read preference to allow secondary reads'
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

        it_behaves_like 'sends user-specified read preference'
      end

      context 'when the server is a replica set member' do

        let(:standalone?) { false }
        let(:mongos?) { false }

        it_behaves_like 'sends user-specified read preference'
      end
    end
  end
end
