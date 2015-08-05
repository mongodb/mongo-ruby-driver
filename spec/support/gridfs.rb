# Copyright (C) 2014-2015 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Matcher for determining if the results of the opeartion match the
# test's expected results.
#
# @since 2.0.0

# Matcher for determining if the collection's data matches the
# test's expected collection data.
#
# @since 2.1.0
RSpec::Matchers.define :completes_successfully do |test|

  match do |actual|
    if actual.is_a?(Exception)
      raise actual
    else
      actual.is_a?(BSON::ObjectId)
    end
  end
end

RSpec::Matchers.define :match_chunks_collection do |expected|

  match do |actual|
    if expected
      actual.find.all? do |doc|
        if matching_doc = expected.find(files_id: doc['files_id'], n: doc['n']).first
          matching_doc.all? do |k, v|
            doc[k] == v || k == '_id'
          end
        else
          false
        end
      end
    else
      actual.find.to_a.empty?
    end
  end
end

RSpec::Matchers.define :raise_correct_error do |test|

  match do |actual|
    
  end
end

RSpec::Matchers.define :match_files_collection do |expected|

  match do |actual|
    actual.find.all? do |doc|
      if matching_doc = expected.find(_id: doc['_id']).first
        matching_doc.all? do |k, v|
          doc[k] == v
        end
      else
        false
      end
    end
  end
end


module Mongo
  module GridFS

    # Represents a GridFS specification test.
    #
    # @since 2.0.0
    class Spec

      # @return [ String ] description The spec description.
      #
      # @since 2.0.0
      attr_reader :description

      # Instantiate the new spec.
      #
      # @example Create the spec.
      #   Spec.new(file)
      #
      # @param [ String ] file The name of the file.
      #
      # @since 2.1.0
      def initialize(file)
        @spec = YAML.load(ERB.new(File.new(file).read).result)
        @description = File.basename(file)
        @data = @spec['data']
      end

      # Get a list of CRUDTests for each test definition.
      #
      # @example Get the list of CRUDTests.
      #   spec.tests
      #
      # @return [ Array<CRUDTest> ] The list of CRUDTests.
      #
      # @since 2.1.0
      def tests
        @tests ||= @spec['tests'].collect do |test|
          Mongo::GridFS::GridFSTest.new(@data, test)
        end
      end
    end

    # Represents a single GridFS test.
    #
    # @since 2.1.0
    class GridFSTest

      # The test description.
      #
      # @return [ String ] description The test description.
      #
      # @since 2.1.0
      attr_reader :description
      attr_reader :expected_files_collection
      attr_reader :expected_chunks_collection

      # Instantiate the new GridFSTest.
      #
      # @example Create the test.
      #   GridFSTest.new(data, test)
      #
      # @param [ Array<Hash> ] data The documents the files and chunks
      # collections must have before the test runs.
      # @param [ Hash ] test The test specification.
      #
      # @since 2.0.0
      def initialize(data, test)
        @data = data
        @description = test['description']
        @act = test['act']
        @arrange = test['arrange']
        @assertion = test['assert']
      end

      def error?
        @assertion['error']
      end

      def run(fs)
        begin
          @files_id = send(@act['operation'], fs)
        rescue => ex
          ex
        ensure
          prepare_expected_data(fs)
        end
      end

      def arrange
      end

      def clear_collections
        expected_files_collection.delete_many if expected_files_collection
        expected_chunks_collection.delete_many if expected_chunks_collection
      end

      private

      def to_hex(string)
        [ string ].pack('H*')
      end

      def transform_files_docs(docs)
        @expected_files = docs.collect do |doc|
          doc['_id'] = @files_id if @files_id 
          doc['uploadDate'] = upload_date
          doc
        end
      end

      def to_binary(data)
        data.is_a?(BSON::Binary) ? data : BSON::Binary.new(to_hex(data['$hex']), :generic)
      end

      def transform_chunks_docs(docs)
        @expected_chunks = docs.collect do |doc|
          doc['_id'] = BSON::ObjectId.new
          doc['files_id'] = @files_id if @files_id
          doc['data'] = to_binary(doc['data']) if doc['data']
          doc
        end
      end

      def insert_expected_files(fs, data)
        if data['insert'] =~ /\.files/
          @expected_files_collection ||= fs.database['expected.files']
          @expected_files_collection.insert_many(transform_files_docs(data['documents']))
        end
      end

      def insert_expected_chunks(fs, data)
        if data['insert'] =~ /\.chunks/
          @expected_chunks_collection ||= fs.database['expected.chunks']
          @expected_chunks_collection.insert_many(transform_chunks_docs(data['documents']))
        end
      end

      def prepare_collections(fs, data)
        insert_expected_files(fs, data)
        insert_expected_chunks(fs, data)
      end

      def prepare_expected_data(fs)
        @assertion['data'].each do |data|
          prepare_collections(fs, data)
        end
      end

      def options
        @act['arguments']['options'].reduce({}) do |opts, (k, v)|
          opts.merge!(chunk_size: v) if k == "chunkSizeBytes"
          opts.merge!(upload_date: upload_date)
          opts.merge!(content_type: v) if k == "contentType"
          opts.merge!(metadata: v) if k == "metadata"
          opts
        end
      end

      def upload_date
        @upload_date ||= Time.now
      end

      def filename
        @act['arguments']['filename']
      end

      def upload(fs)
        io = StringIO.new(to_hex(@act['arguments']['source']['$hex']))
        fs.upload_from_stream(filename, io, options)
      end
    end
  end
end
