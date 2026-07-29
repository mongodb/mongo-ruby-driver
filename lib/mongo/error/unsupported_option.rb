# frozen_string_literal: true

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
      # The error message provided when the user passes the allow_disk_use
      # option to a find operation against a server that does not support the
      # allow_disk_use operation and does not provide option validation.
      #
      # @api private
      ALLOW_DISK_USE_MESSAGE = 'The MongoDB server handling this request does ' \
                               'not support the allow_disk_use option on this command. The ' \
                               'allow_disk_use option is supported on find commands on MongoDB ' \
                               'server versions 4.4 and later'

      # The error message provided when the user passes the commit_quorum option
      # to a createIndexes operation against a server that does not support
      # that option.
      #
      # @api private
      COMMIT_QUORUM_MESSAGE = 'The MongoDB server handling this request does ' \
                              'not support the commit_quorum option on this command. The commit_quorum ' \
                              'option is supported on createIndexes commands on MongoDB server versions ' \
                              '4.4 and later'

      # Raise an error about an unsupported allow_disk_use option.
      #
      # @return [ Mongo::Error::UnsupportedOption ] An error with a default
      #   error message.
      #
      # @api private
      def self.allow_disk_use_error
        new(ALLOW_DISK_USE_MESSAGE)
      end

      # Raise an error about an unsupported commit_quorum option.
      #
      # @return [ Mongo::Error::UnsupportedOption ] An error with a default
      #   error message.
      #
      # @api private
      def self.commit_quorum_error
        new(COMMIT_QUORUM_MESSAGE)
      end
    end
  end
end
