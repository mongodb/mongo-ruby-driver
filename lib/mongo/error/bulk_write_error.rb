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

module Mongo
  class Error

    # Exception raised if there are write errors upon executing a bulk
    # operation.
    #
    # Unlike OperationFailure, BulkWriteError does not currently expose
    # individual error components (such as the error code). The result document
    # (which can be obtained using the +result+ attribute) provides detailed
    # error information and can be examined by the application if desired.
    #
    # @note A bulk operation that resulted in a BulkWriteError may have
    #   written some of the documents to the database. If the bulk write
    #   was unordered, writes may have also continued past the write that
    #   produced a BulkWriteError.
    #
    # @since 2.0.0
    class BulkWriteError < Error

      # @return [ BSON::Document ] result The error result.
      attr_reader :result

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::Error::BulkWriteError.new(response)
      #
      # @param [ Hash ] result A processed response from the server
      #   reporting results of the operation.
      #
      # @since 2.0.0
      def initialize(result)
        @result = result
        # Exception constructor behaves differently for a nil argument and
        # for no argument. Avoid passing nil explicitly.
        super(*[build_message])
      end

      private

      # Generates an error message when there are multiple write errors.
      #
      # @example Multiple documents fail validation
      #
      # col has validation { 'validator' => { 'x' => { '$type' => 'string' } } }
      # col.insert_many([{_id: 1}, {_id: 2}], ordered: false)
      #
      # Multiple errors:
      #   [121]: Document failed validation --
      #     {"failingDocumentId":1,"details":{"operatorName":"$type",
      #     "specifiedAs":{"x":{"$type":"string"}},"reason":"field was
      #     missing"}};
      #   [121]: Document failed validation --
      #     {"failingDocumentId":2, "details":{"operatorName":"$type",
      #     "specifiedAs":{"x":{"$type":"string"}}, "reason":"field was
      #     missing"}}
      #
      # @return [ String ] The error message
      def build_message
        errors = @result['writeErrors']
        return nil unless errors

        fragment = ""
        cut_short = false
        errors.first(10).each_with_index do |error, i|
          fragment += "; " if fragment.length > 0
          fragment += "[#{error['code']}]: #{error['errmsg']}"
          fragment += " -- #{error['errInfo'].to_json}" if error['errInfo']

          if fragment.length > 3000
            cut_short = i < [9, errors.length].min
            break
          end
        end

        fragment += '...' if errors.length > 10 || cut_short

        if errors.length > 1
          fragment = "Multiple errors: #{fragment}"
        end

        fragment
      end
    end
  end
end
