# frozen_string_literal: true
# encoding: utf-8

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

      # A MongoDB delete operation sent as a legacy wire protocol message.
      #
      # @api private
      #
      # @since 2.5.2
      class Legacy
        include Specifiable
        include Executable
        include PolymorphicResult
        include Validatable

        private

        def selector(connection)
          # This returns the first delete.
          # The driver only puts one delete into the list normally, so this
          # doesn't discard operations.
          send(IDENTIFIER).first.tap do |selector|
            validate_find_options(connection, selector)
          end
        end

        def message(connection)
          selector = selector(connection)
          opts = (selector[Operation::LIMIT] || 0) <= 0 ? {} : { :flags => [ :single_remove ] }
          Protocol::Delete.new(db_name, coll_name, selector[Operation::Q], opts)
        end

        def gle
          wc = write_concern ||  WriteConcern.get(WriteConcern::DEFAULT)
          if gle_message = wc.get_last_error
            Protocol::Query.new(
                db_name,
                Database::COMMAND,
                gle_message,
                options.merge(limit: -1)
            )
          end
        end
      end
    end
  end
end
