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
  class Error

    # Raised if an unsupported option is specified for an operation.
    class UnsupportedOption < Error
      # @api private
      HINT_MESSAGE = "The MongoDB server handling this request does not support the hint " \
        "option on this command. The hint option is supported on update commands " \
        "on MongoDB server versions 4.2 and later and on findAndModify and delete " \
        "commands on MongoDB server versions 4.4 and later"

      # @api private
      ALLOW_DISK_USE_MESSAGE = "The MongoDB server handling this request does not support the allow_disk_use " \
        "option on this command. The allow_disk_use option is supported on find commands " \
        "on MongoDB server versions 4.4 and later"

      def self.hint_error(**options)
        unacknowledged_write = options[:unacknowledged_write] || false

        error_message = if unacknowledged_write
          self.unacknowledged_write_message('hint')
        else
          HINT_MESSAGE
        end

        self.new(error_message)
      end

      def self.allow_disk_use_error
        self.new(ALLOW_DISK_USE_MESSAGE)
      end

      private

      def self.unacknowledged_write_message(option_name)
        "The #{option_name} option cannot be specified on an unacknowledged " \
        "write operation. Remove the #{option_name} option or perform this " \
        "operation with a write concern of at least { w: 1 }"
      end
    end
  end
end
