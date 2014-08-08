# Copyright (C) 2009-2014 MongoDB, Inc.
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
  module Operation

    # Defines behaviour for responses that must be verified for errors.
    #
    # @since 2.0.0
    module Verifiable

      # The number of documents updated in the write.
      #
      # @since 2.0.0
      N = 'n'.freeze

      # The ok status field in the response.
      #
      # @since 2.0.0
      OK = 'ok'.freeze

      # @return [ Protocol::Reply ] reply The wrapped wire protocol reply.
      attr_reader :reply

      # Initialize a new verifiable response.
      #
      # @example Instantiate the verifiable.
      #   Verifiable.new(reply, 0)
      #
      # @param [ Protocol::Reply ] reply The wire protocol reply.
      # @param [ Integer ] count The number of documents affected.
      #
      # @since 2.0.0
      def initialize(reply, count = nil)
        @reply = reply
        @count = count
      end

      # Get the number of documents affected.
      #
      # @example Get the number affected.
      #   verifiable.count
      #
      # @return [ Integer ] The number affected.
      #
      # @since 2.0.0
      def count
        @count || first[N]
      end
      alias :n :count

      # If the response was a command then determine if it was considered a
      # success.
      #
      # @example Was the command ok?
      #   verifiable.ok?
      #
      # @return [ true, false ] If the command was ok.
      #
      # @since 2.0.0
      def ok?
        first[OK] == 1 || reply.nil?
      end

      private

      def command_failure?
        reply && (!ok? || errors?)
      end

      def documents
        reply ? reply.documents : []
      end

      def errors?
        first[Operation::ERROR] && first[Operation::ERROR_CODE]
      end

      def first
        @first ||= documents[0] || {}
      end

      def write_concern_errors
        first[Write::WRITE_CONCERN_ERROR] || []
      end

      def write_concern_errors?
        !write_concern_errors.empty?
      end

      def write_errors
        first[Write::WRITE_ERRORS] || []
      end

      def write_errors?
        !write_errors.empty?
      end

      def write_failure?
        reply && (command_failure? || write_errors? || write_concern_errors?)
      end
    end
  end
end
