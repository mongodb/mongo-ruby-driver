# Copyright (C) 2015-2017 MongoDB, Inc.
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

    # Raised when a socket has an error.
    #
    # @since 2.0.0
    class SocketError < Error

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::Error::SocketError.new(msg)
      #
      # @param [ String ] msg The error message.
      #
      # @since 2.0.0
      def initialize(msg = nil, labels = nil)
        @labels = labels || []
        super(msg) if msg
      end

      # Does the error have the given label?
      #
      # @example
      #   error.label?(label)
      #
      # @return [ true, false ] Whether the error has the given label.
      #
      # @since 2.6.0
      def label?(label)
        @labels.include?(label)
      end

      private

      def add_label(label)
        @labels << label unless label?(label)
      end
    end
  end
end
