# frozen_string_literal: true
# encoding: utf-8

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

      def build_message
        errors = @result['writeErrors']
        return nil unless errors

        fragment = errors.first(10).map do |error|
          "[#{error['code']}]: #{error['errmsg']}"
        end.join('; ')

        fragment += '...' if errors.length > 10

        if errors.length > 1
          fragment = "Multiple errors: #{fragment}"
        end

        fragment
      end
    end
  end
end
