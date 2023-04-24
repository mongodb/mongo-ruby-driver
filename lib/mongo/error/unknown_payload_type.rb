# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2017-2020 MongoDB Inc.
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

    # Raised if an unknown payload type is encountered when an OP_MSG is created or read.
    #
    # @since 2.5.0
    class UnknownPayloadType < Error

      # The error message.
      #
      # @since 2.5.0
      MESSAGE = 'Unknown payload type (%s) encountered when creating or reading an OP_MSG wire protocol message.'

      # Create the new exception.
      #
      # @example Create the new exception.
      #   Mongo::Error::UnknownPayloadType.new(byte)
      #
      # @param [ String ] byte The unknown payload type.
      #
      # @since 2.5.0
      def initialize(byte)
        super(MESSAGE % byte.inspect)
      end
    end
  end
end
