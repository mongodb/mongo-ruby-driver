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

    # Exception that is raised when trying to serialize a document that
    # exceeds max BSON object size.
    #
    # @since 2.0.0
    class MaxBSONSize < Error

      # The message is constant.
      #
      # @since 2.0.0
      MESSAGE = "The document exceeds maximum allowed BSON size".freeze

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::Error::MaxBSONSize.new(max)
      #
      # @param [ String | Numeric ] max_size_or_msg The message to use or
      #   the maximum size to insert into the predefined message. The
      #   Numeric argument type is deprecated.
      #
      # @since 2.0.0
      def initialize(max_size_or_msg = nil)
        if max_size_or_msg.is_a?(Numeric)
          msg = "#{MESSAGE}. The maximum allowed size is #{max_size_or_msg}"
        elsif max_size_or_msg
          msg = max_size_or_msg
        else
          msg = MESSAGE
        end
        super(msg)
      end
    end
  end
end
