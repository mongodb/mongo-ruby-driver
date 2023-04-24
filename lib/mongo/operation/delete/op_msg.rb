# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2018-2020 MongoDB Inc.
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
    class Delete

      # A MongoDB delete operation sent as an op message.
      #
      # @api private
      #
      # @since 2.5.2
      class OpMsg < OpMsgBase
        include BypassDocumentValidation
        include ExecutableNoValidate
        include ExecutableTransactionLabel
        include PolymorphicResult
        include Validatable

        private

        def selector(connection)
          { delete: coll_name,
            Protocol::Msg::DATABASE_IDENTIFIER => db_name,
            ordered: ordered?,
            let: spec[:let],
            comment: spec[:comment],
          }.compact.tap do |selector|
            if hint = spec[:hint]
              validate_hint_on_update(connection, selector)
              selector[:hint] = hint
            end
          end
        end

        def message(connection)
          section = Protocol::Msg::Section1.new(IDENTIFIER, send(IDENTIFIER))
          Protocol::Msg.new(flags, {}, command(connection), section)
        end
      end
    end
  end
end
