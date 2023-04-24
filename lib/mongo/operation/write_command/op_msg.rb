# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2021 MongoDB Inc.
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
    class WriteCommand

      # A MongoDB write command operation sent as an op message.
      #
      # @api private
      class OpMsg < OpMsgBase
        include Validatable

        private

        def selector(connection)
          super.tap do |selector|
            if selector.key?(:findAndModify)
              validate_find_options(connection, selector)
            end
            if wc = spec[:write_concern]
              selector[:writeConcern] = wc.options
            end
          end
        end
      end
    end
  end
end
