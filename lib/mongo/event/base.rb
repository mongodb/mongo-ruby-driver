# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2018-2020 MongoDB Inc.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  module Event

    # Base class for all events.
    #
    # @since 2.6.0
    class Base
      # Returns a concise yet useful summary of the event.
      # Meant to be overridden in derived classes.
      #
      # @return [ String ] String summary of the event.
      #
      # @note This method is experimental and subject to change.
      #
      # @since 2.7.0
      # @api experimental
      def summary
        "#<#{self.class}>"
      end

      private

      def short_class_name
        self.class.name.sub(/^Mongo::Monitoring::Event::/, '')
      end
    end
  end
end
