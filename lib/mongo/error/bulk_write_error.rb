# Copyright (C) 2014-2019 MongoDB, Inc.
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

    # Exception raised if there are write errors upon executing the bulk
    # operation.
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
      end

      def to_s
        messages = if errors = result['writeErrors']
          frag = ': ' + errors[0..10].map do |error|
            "#{error['errmsg']} (#{error['code']})"
          end.join(', ')
          if errors.length > 10
            frag += '...'
          else
            frag
          end
        else
          ''
        end
        "#{self.class}: #{messages}" + notes_tail
      end
    end
  end
end
