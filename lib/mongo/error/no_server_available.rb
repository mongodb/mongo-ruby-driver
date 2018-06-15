# Copyright (C) 2014-2017 MongoDB, Inc.
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

    # Raised if there are no servers available matching the preference.
    #
    # @since 2.0.0
    class NoServerAvailable < Error

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::Error::NoServerAvailable.new(server_selector)
      #
      # @param [ Hash ] server_selector The server preference that could not be
      #   satisfied.
      # @param [ Array<String> ] labels A set of labels describing the error.
      #
      # @since 2.0.0
      def initialize(server_selector, labels = nil)
        @labels = labels || []
        super("No server is available matching preference: #{server_selector.inspect} " +
                "using server_selection_timeout=#{server_selector.server_selection_timeout} " +
                "and local_threshold=#{server_selector.local_threshold}")
      end

      # Does the error have the given label?
      #
      # @example
      #   error.label?(label)
      #
      # @param [ String ] label The label to check if the error has.
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
