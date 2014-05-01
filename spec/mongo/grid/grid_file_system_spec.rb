require 'spec_helper'

describe Mongo::Grid::GridFileSystem do
  include_context 'gridfs implementation'

  it_behaves_like 'a storable object'

  describe '#open' do

    before(:each) do
      f = grid.open(filename, 'r')
    end

    it 'creates a new version of the file' do
      pending 'collection implementation'
    end

    context 'when max_versions is set' do

      context 'when we are at max_versions versions' do

        it 'deletes the oldest version of the file' do
          pending 'collection implementation'
        end
      end

      context 'when we are below max_versions versions' do

        it 'does not delete any old versions of the file' do
          pending 'collection implementation'
        end
      end
    end

    context 'when max_versions is not set' do

      it 'does not delete any old versions of the file' do
        pending 'collection implementation'
      end
    end
  end

  describe '#delete_old_versions' do

    context 'when cutoff is a Date' do

      it 'deletes versions older than Date' do
        pending 'collection implementation'
      end

      it 'leaves versions more recent than Date' do
        pending 'collection implementation'
      end

      it 'returns an Integer' do
        pending 'collection implementation'
      end

      it 'returns the number of versions removed' do
        pending 'collection implementation'
      end
    end

    context 'when cutoff is an Integer' do

      it 'leaves cutoff versions in the system' do
        pending 'collection implementation'
      end

      it 'removes the oldest versions until only cutoff versions remain' do
        pending 'collection implementation'
      end

      it 'returns an Integer' do
        pending 'collection implementation'
      end

      it 'returns the number of versions removed' do
        pending 'collection implementation'
      end
    end
  end
end
