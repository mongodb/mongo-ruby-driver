# Copyright (C) 2020 MongoDB Inc.
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

  module ServerSelector

    # @api private
    class Base
      private

      # Convert this server preference definition into a format appropriate
      #   for sending to a MongoDB server (i.e., as a command field).
      #
      # @return [ Hash ] The server preference formatted as a command field value.
      #
      # @since 2.0.0
      def full_doc
        @full_doc ||= begin
          preference = { :mode => self.class.const_get(:SERVER_FORMATTED_NAME) }
          preference.update(tags: tag_sets) unless tag_sets.empty?
          preference.update(maxStalenessSeconds: max_staleness) if max_staleness
          preference
        end
      end
    end
  end
end
