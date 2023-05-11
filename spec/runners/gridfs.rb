# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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


# Matcher for determining whether the operation completed successfully.
#
# @since 2.1.0
RSpec::Matchers.define :completes_successfully do |test|

  match do |actual|
    actual == test.expected_result || test.expected_result.nil?
  end
end

# Matcher for determining whether the actual chunks collection matches
# the expected chunks collection.
#
# @since 2.1.0
RSpec::Matchers.define :match_chunks_collection do |expected|

  match do |actual|
    return true if expected.nil?
    if expected.find.to_a.empty?
      actual.find.to_a.empty?
    else
      actual.find.all? do |doc|
        if matching_doc = expected.find(files_id: doc['files_id'], n: doc['n']).first
          matching_doc.all? do |k, v|
            doc[k] == v || k == '_id'
          end
        else
          false
        end
      end
    end
  end
end

# Matcher for determining whether the actual files collection matches
# the expected files collection.
#
# @since 2.1.0
RSpec::Matchers.define :match_files_collection do |expected|

  match do |actual|
    return true if expected.nil?
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

# Matcher for determining whether the operation raised the correct error.
#
# @since 2.1.0
RSpec::Matchers.define :match_error do |error|

  match do |actual|
    Mongo::GridFS::Test::ERROR_MAPPING[error] == actual.class
  end
end


module Mongo
  module GridFS

    # Represents a GridFS specification test.
    #
    # @since 2.1.0
    class Spec

      # @return [ String ] description The spec description.
      #
      # @since 2.1.0
      attr_reader :description

      # Instantiate the new spec.
      #
      # @param [ String ] test_path The path to the file.
      #
      # @since 2.1.0
      def initialize(test_path)
        @spec = ::Utils.load_spec_yaml_file(test_path)
        @description = File.basename(test_path)
        @data = @spec['data']
      end

      # Get a list of Tests for each test definition.
      #
      # @example Get the list of Tests.
      #   spec.tests
      #
      # @return [ Array<Test> ] The list of Tests.
      #
      # @since 2.1.0
      def tests
        @tests ||= @spec['tests'].collect do |test|
          Test.new(@data, test)
        end
      end
    end

    # Contains shared helper functions for converting YAML test values to Ruby objects.
    #
    # @since 2.1.0
    module Convertible

      # Convert an integer to the corresponding CRUD method suffix.
      #
      # @param [ Integer ] int The limit.
      #
      # @return [ String ] The CRUD method suffix.
      #
      # @since 2.1.0
      def limit(int)
        int == 0 ? 'many' : 'one'
      end

      # Convert an id value to a BSON::ObjectId.
      #
      # @param [ Object ] v The value to convert.
      # @param [ Hash ] opts The options.
      #
      # @option opts [ BSON::ObjectId ] :id The id override.
      #
      # @return [ BSON::ObjectId ] The object id.
      #
      # @since 2.1.0
      def convert__id(v, opts = {})
        to_oid(v, opts[:id])
      end

      # Convert a value to a date.
      #
      # @param [ Object ] v The value to convert.
      # @param [ Hash ] opts The options.
      #
      # @return [ Time ] The upload date time value.
      #
      # @since 2.1.0
      def convert_uploadDate(v, opts = {})
        v.is_a?(Time) ? v : v['$date'] ? Time.parse(v['$date']) : upload_date
      end

      # Convert an file id value to a BSON::ObjectId.
      #
      # @param [ Object ] v The value to convert.
      # @param [ Hash ] opts The options.
      #
      # @option opts [ BSON::ObjectId ] :id The id override.
      #
      # @return [ BSON::ObjectId ] The object id.
      #
      # @since 2.1.0
      def convert_files_id(v, opts = {})
        to_oid(v, opts[:files_id])
      end

      # Convert a value to BSON::Binary data.
      #
      # @param [ Object ] v The value to convert.
      # @param [ Hash ] opts The options.
      #
      # @return [ BSON::Binary ] The converted data.
      #
      # @since 2.1.0
      def convert_data(v, opts = {})
        v.is_a?(BSON::Binary) ? v : BSON::Binary.new(to_hex(v['$hex'], opts), :generic)
      end

      # Transform documents to have the correct object types for serialization.
      #
      # @param [ Array<Hash> ] docs The documents to transform.
      # @param [ Hash ] opts The options.
      #
      # @return [ Array<Hash> ] The transformed documents.
      #
      # @since 2.1.0
      def transform_docs(docs, opts = {})
        docs.collect do |doc|
          doc.each do |k, v|
            doc[k] = send("convert_#{k}", v, opts) if respond_to?("convert_#{k}")
          end
          doc
        end
      end

      # Convert a string to a hex value.
      #
      # @param [ String ] string The value to convert.
      # @param [ Hash ] opts The options.
      #
      # @return [ String ] The hex value.
      #
      # @since 2.1.0
      def to_hex(string, opts = {})
        [ string ].pack('H*')
      end

      # Convert an object id represented in json to a BSON::ObjectId.
      # A new BSON::ObjectId is returned if the json document is empty.
      #
      # @param [ Object ] value The value to convert.
      # @param [ Object ] id The id override.
      #
      # @return [ BSON::ObjectId ] The object id.
      #
      # @since 2.1.0
      def to_oid(value, id = nil)
        if id
          id
        elsif value.is_a?(BSON::ObjectId)
          value
        elsif value['$oid']
          BSON::ObjectId.from_string(value['$oid'])
        else
          BSON::ObjectId.new
        end
      end

      # Convert options.
      #
      # @return [ Hash ] The options.
      #
      # @since 2.1.0
      def options
        @act['arguments']['options'].reduce({}) do |opts, (k, v)|
          opts.merge!(chunk_size: v) if k == "chunkSizeBytes"
          opts.merge!(upload_date: upload_date)
          opts.merge!(content_type: v) if k == "contentType"
          opts.merge!(metadata: v) if k == "metadata"
          opts
        end
      end
    end

    # Represents a single GridFS test.
    #
    # @since 2.1.0
    class Test
      include Convertible
      extend Forwardable

      def_delegators :@operation, :expected_files_collection,
                                  :expected_chunks_collection,
                                  :result,
                                  :expected_error,
                                  :expected_result,
                                  :error?

      # The test description.
      #
      # @return [ String ] The test description.
      #
      # @since 2.1.0
      attr_reader :description

      # The upload date to use in the test.
      #
      # @return [ Time ] The upload date.
      #
      # @since 2.1.0
      attr_reader :upload_date

      # Mapping of test error strings to driver classes.
      #
      # @since 2.1.0
      ERROR_MAPPING = {
          'FileNotFound' => Mongo::Error::FileNotFound,
          'ChunkIsMissing' => Mongo::Error::MissingFileChunk,
          'ChunkIsWrongSize' => Mongo::Error::UnexpectedChunkLength,
          'ExtraChunk' => Mongo::Error::ExtraFileChunk,
          'RevisionNotFound' => Mongo::Error::InvalidFileRevision
      }

      # Instantiate the new GridFS::Test.
      #
      # @example Create the test.
      #   Test.new(data, test)
      #
      # @param [ Array<Hash> ] data The documents the files and chunks
      #   collections must have before the test runs.
      # @param [ Hash ] test The test specification.
      #
      # @since 2.1.0
      def initialize(data, test)
        @pre_data = data
        @description = test['description']
        @upload_date = Time.now
        if test['assert']['error']
          @operation = UnsuccessfulOp.new(self, test)
        else
          @operation = SuccessfulOp.new(self, test)
        end
        @result = nil
      end

      # Whether the expected  and actual collections should be compared after the test runs.
      #
      # @return [ true, false ] Whether the actual and expected collections should be compared.
      #
      # @since 2.1.0
      def assert_data?
        @operation.assert['data']
      end

      # Run the test.
      #
      # @example Run the test
      #   test.run(fs)
      #
      # @param [ Mongo::Grid::FSBucket ] fs The Grid::FSBucket to use in the test.
      #
      # @since 2.1.0
      def run(fs)
        clear_collections(fs)
        setup(fs)
        @operation.run(fs)
      end

      # Clear the files and chunks collection in the FSBucket and other collections used in the test.
      #
      # @example Clear the test collections
      #   test.clear_collections(fs)
      #
      # @param [ Mongo::Grid::FSBucket ] fs The Grid::FSBucket whose collections should be cleared.
      #
      # @since 2.1.0
      def clear_collections(fs)
        fs.files_collection.delete_many
        fs.files_collection.indexes.drop_all rescue nil
        fs.chunks_collection.delete_many
        fs.chunks_collection.indexes.drop_all rescue nil
        #@operation.clear_collections(fs)
      end

      private

      def setup(fs)
        insert_pre_data(fs)
        @operation.arrange(fs)
      end

      def files_data
        @files_data ||= transform_docs(@pre_data['files'])
      end

      def chunks_data
        @chunks_data ||= transform_docs(@pre_data['chunks'])
      end

      def insert_pre_files_data(fs)
        fs.files_collection.insert_many(files_data)
        fs.database['expected.files'].insert_many(files_data) if assert_data?
      end

      def insert_pre_chunks_data(fs)
        fs.chunks_collection.insert_many(chunks_data)
        fs.database['expected.chunks'].insert_many(chunks_data) if assert_data?
      end

      def insert_pre_data(fs)
        insert_pre_files_data(fs) unless files_data.empty?
        insert_pre_chunks_data(fs) unless chunks_data.empty?
      end

      # Contains logic and helper methods shared between a successful and
      # non-successful GridFS test operation.
      #
      # @since 2.1.0
      module Operable
        extend Forwardable

        def_delegators :@test, :upload_date

        # The test operation name.
        #
        # @return [ String ] The operation name.
        #
        # @since 2.1.0
        attr_reader :op

        # The test assertion.
        #
        # @return [ Hash ] The test assertion definition.
        #
        # @since 2.1.0
        attr_reader :assert

        # The operation result.
        #
        # @return [ Object ] The operation result.
        #
        # @since 2.1.0
        attr_reader :result

        # The collection containing the expected files.
        #
        # @return [ Mongo::Collection ] The expected files collection.
        #
        # @since 2.1.0
        attr_reader :expected_files_collection

        # The collection containing the expected chunks.
        #
        # @return [ Mongo::Collection ] The expected chunks collection.
        #
        # @since 2.1.0
        attr_reader :expected_chunks_collection

        # Instantiate the new test operation.
        #
        # @example Create the test operation.
        #   Test.new(data, test)
        #
        # @param [ Test ] test The test.
        # @param [ Hash ] spec The test specification.
        #
        # @since 2.1.0
        def initialize(test, spec)
          @test = test
          @arrange = spec['arrange']
          @act = spec['act']
          @op = @act['operation']
          @arguments = @act['arguments']
          @assert = spec['assert']
        end

        # Arrange the data before running the operation.
        # This sets up the correct scenario for the test.
        #
        # @example Arrange the data.
        #   operation.arrange(fs)
        #
        # @param [ Grid::FSBucket ] fs The FSBucket used in the test.
        #
        # @since 2.1.0
        def arrange(fs)
          if @arrange
            @arrange['data'].each do |data|
              send("#{data.keys.first}_exp_data", fs, data)
            end
          end
        end

        # Run the test operation.
        #
        # @example Execute the operation.
        #   operation.run(fs)
        #
        # @param [ Grid::FSBucket ] fs The FSBucket used in the test.
        #
        # @result [ Object ] The operation result.
        #
        # @since 2.1.0
        def run(fs)
          @expected_files_collection = fs.database['expected.files']
          @expected_chunks_collection = fs.database['expected.chunks']
          act(fs)
          prepare_expected_collections(fs)
          result
        end

        private

        def prepare_expected_collections(fs)
          if @test.assert_data?
            @assert['data'].each do |data|
              op = "#{data.keys.first}_exp_data"
              send(op, fs, data)
            end
          end
        end

        def insert_exp_data(fs, data)
          coll = fs.database[data['insert']]
          if coll.name =~ /.files/
            opts = { id: @result }
          else
            opts = { files_id: @result }
          end
          coll.insert_many(transform_docs(data['documents'], opts))
        end

        def delete_exp_data(fs, data)
          coll = fs.database[data['delete']]
          data['deletes'].each do |del|
            id = del['q'].keys.first
            coll.find(id => to_oid(del['q'][id])).send("delete_#{limit(del['limit'])}")
          end
        end

        def update_exp_data(fs, data)
          coll = fs.database[data['update']]
          data['updates'].each do |update|
            sel = update['q'].merge('files_id' => to_oid(update['q']['files_id']))
            data = BSON::Binary.new(to_hex(update['u']['$set']['data']['$hex']), :generic)
            u = update['u'].merge('$set' => { 'data' => data })
            coll.find(sel).update_one(u)
          end
        end

        def upload(fs)
          io = StringIO.new(to_hex(@arguments['source']['$hex']))
          fs.upload_from_stream(@arguments['filename'], io, options)
        end

        def download(fs)
          io = StringIO.new.set_encoding(BSON::BINARY)
          fs.download_to_stream(to_oid(@arguments['id']), io)
          io.string
        end

        def download_by_name(fs)
          io = StringIO.new.set_encoding(BSON::BINARY)
          if @arguments['options']
            fs.download_to_stream_by_name(@arguments['filename'], io, revision: @arguments['options']['revision'])
          else
            fs.download_to_stream_by_name(@arguments['filename'], io)
          end
          io.string
        end

        def delete(fs)
          fs.delete(to_oid(@arguments['id']))
        end
      end

      # A GridFS test operation that is expected to succeed.
      #
      # @since 2.1.0
      class SuccessfulOp
        include Convertible
        include Test::Operable

        # The expected result of executing the operation.
        #
        # @example Get the expected result.
        #   operation.expected_result
        #
        # @result [ Object ] The operation result.
        #
        # @since 2.1.0
        def expected_result
          if @assert['result'] == '&result'
            @result
          elsif @assert['result'] != 'void'
            to_hex(@assert['result']['$hex'])
          end
        end

        # Execute the operation.
        #
        # @example Execute the operation.
        #   operation.act(fs)
        #
        # @param [ Grid::FSBucket ] fs The FSBucket used in the test.
        #
        # @result [ Object ] The operation result.
        #
        # @since 2.1.0
        def act(fs)
          @result = send(op, fs)
        end

        # Whether this operation is expected to raise an error.
        #
        # @return [ false ] The operation is expected to succeed.
        #
        # @since 2.1.0
        def error?
          false
        end
      end

      class UnsuccessfulOp
        include Convertible
        include Test::Operable

        # Whether this operation is expected to raise an error.
        #
        # @return [ true ] The operation is expected to fail.
        #
        # @since 2.1.0
        def error?
          true
        end

        # The expected error.
        #
        # @example Execute the operation.
        #   operation.expected_error
        #
        # @return [ String ] The expected error name.
        #
        # @since 2.1.0
        def expected_error
          @assert['error']
        end

        # Execute the operation.
        #
        # @example Execute the operation.
        #   operation.act(fs)
        #
        # @param [ Grid::FSBucket ] fs The FSBucket used in the test.
        #
        # @result [ Mongo::Error ] The error encountered.
        #
        # @since 2.1.0
        def act(fs)
          begin
            send(op, fs)
          rescue => ex
            @result = ex
          end
        end
      end
    end
  end
end
