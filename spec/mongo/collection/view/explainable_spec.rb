# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Collection::View::Explainable do

  let(:selector) do
    {}
  end

  let(:options) do
    {}
  end

  let(:view) do
    Mongo::Collection::View.new(authorized_collection, selector, options)
  end

  before do
    authorized_collection.delete_many
  end

  describe '#explain' do

    shared_examples 'executes the explain' do
      context '3.0+ server' do
        min_server_fcv '3.0'

        context 'not sharded' do
          require_topology :single, :replica_set

          it 'executes the explain' do
            explain[:queryPlanner][:namespace].should == authorized_collection.namespace
          end
        end

        context 'sharded' do
          require_topology :sharded

          context 'pre-3.2 server' do
            max_server_version '3.0'

            it 'executes the explain' do
              explain[:queryPlanner][:parsedQuery].should be_a(Hash)
            end
          end

          context '3.2+ server' do
            min_server_fcv '3.2'

            it 'executes the explain' do
              explain[:queryPlanner][:mongosPlannerVersion].should == 1
            end
          end
        end
      end

      context '2.6 server' do
        max_server_version '2.6'

        it 'executes the explain' do
          explain[:cursor].should == 'BasicCursor'
        end
      end
    end

    context 'without arguments' do
      let(:explain) do
        view.explain
      end

      include_examples 'executes the explain'
    end

    context 'with verbosity argument' do
      let(:explain) do
        view.explain(verbosity: verbosity)
      end

      shared_examples 'triggers server error' do
        # 3.0 does not produce the error.
        min_server_fcv '3.2'

        it 'triggers server error' do
          lambda do
            explain
          end.should raise_error(Mongo::Error::OperationFailure, /verbosity string must be|value .* for field .*verbosity.* is not a valid value/)
        end
      end

      context 'valid symbol value' do
        let(:verbosity) { :query_planner }

        include_examples 'executes the explain'
      end

      context 'valid string value' do
        let(:verbosity) { 'executionStats' }

        include_examples 'executes the explain'
      end

      context 'invalid symbol value' do
        let(:verbosity) { :bogus }

        include_examples 'triggers server error'
      end

      context 'invalid string value' do
        let(:verbosity) { 'bogus' }

        include_examples 'triggers server error'
      end
    end
  end
end
