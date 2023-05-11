# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2015-2020 MongoDB Inc.
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

    # Custom behavior for operations that support write concern.
    #
    # @since 2.5.2
    # @api private
    module WriteConcernSupported

      private

      def write_concern_supported?(connection); true; end

      def command(connection)
        add_write_concern!(super, connection)
      end

      def add_write_concern!(sel, connection)
        if write_concern && write_concern_supported?(connection)
          sel[:writeConcern] = write_concern.options
        end
        sel
      end
    end
  end
end
