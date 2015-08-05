require 'spec_helper'

describe 'GridFS' do
  include Mongo::GridFS

  GRIDFS_TESTS.each do |file|

    spec = Mongo::GridFS::Spec.new(file)

    context(spec.description) do

      spec.tests.each do |test|

        context(test.description) do

          before do
            test.arrange
          end

          after do
            fs.files_collection.delete_many
            fs.chunks_collection.delete_many
            test.clear_collections
          end

          let!(:result) do
            test.run(fs)
          end

          let(:fs) do
            authorized_collection.database.fs
          end

          it "raises the correct error", if: test.error? do
            expect(result).to be_a(test.error)
          end

          it 'completes successfully', unless: test.error? do
            expect(result).to completes_successfully
          end

          it 'has the correct documents in the files collection' do
            expect(fs.files_collection).to match_files_collection(test.expected_files_collection)
          end

          it 'has the correct documents in the chunks collection' do
            expect(fs.chunks_collection).to match_chunks_collection(test.expected_chunks_collection)
          end
        end
      end
    end 
  end
end
