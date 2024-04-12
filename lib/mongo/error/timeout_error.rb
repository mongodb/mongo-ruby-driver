# frozen_string_literal: true

# Copyright (C) 2015-present MongoDB Inc.
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
    # Raised when a Client Side Operation Timeout times out.
    class TimeoutError < Error
      # Returns a new TimeoutError with its `original_error` attribute set
      # to the given error.
      #
      # @params [ Exception ] error the original error object
      #
      # @return [ Mongo::Error::TimeoutError ] a new TimeoutError instance.
      def self.wrap(error)
        new.tap do |e|
          e.original_error = error
          error.labels.each { |label| e.add_label(label) }
        end
      end

      # "...drivers MUST expose the underlying error returned from the task
      # from this new error type. The stringified version of the new error type
      # MUST include the stringified version of the underlying error as a
      # substring."
      attr_accessor :original_error

      # Delegates the decision of whether this error is resumable by a change
      # stream, to the original error. If there is no original error, returns
      # falsey.
      #
      # @return [ truthy | falsey ] Whether a change stream can resume from
      #   this error.
      def change_stream_resumable?
        original_error&.change_stream_resumable?
      end

      # Returns the stringified version of the error message. If the
      # `original_error` attribute is set, the description of that error will
      # be appended to this one.
      #
      # @return [ String ] the string representation of the error
      def to_s
        description = super

        description += " (#{original_error})" if original_error

        description
      end
    end
  end
end
