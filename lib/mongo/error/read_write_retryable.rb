# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2019-2022 MongoDB Inc.
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

    # A module encapsulating functionality to indicate whether errors are
    # retryable.
    #
    # @note Although methods of this module are part of the public API,
    #   the fact that these methods are defined on this module and not on
    #   the classes which include this module is not part of the public API.
    #
    # @api semipublic
    module ReadWriteRetryable

      # Error codes and code names that should result in a failing write
      # being retried.
      #
      # @api private
      WRITE_RETRY_ERRORS = [
        {:code_name => 'HostUnreachable', :code => 6},
        {:code_name => 'HostNotFound', :code => 7},
        {:code_name => 'NetworkTimeout', :code => 89},
        {:code_name => 'ShutdownInProgress', :code => 91},
        {:code_name => 'PrimarySteppedDown', :code => 189},
        {:code_name => 'ExceededTimeLimit', :code => 262},
        {:code_name => 'SocketException', :code => 9001},
        {:code_name => 'NotMaster', :code => 10107},
        {:code_name => 'InterruptedAtShutdown', :code => 11600},
        {:code_name => 'InterruptedDueToReplStateChange', :code => 11602},
        {:code_name => 'NotPrimaryNoSecondaryOk', :code => 13435},
        {:code_name => 'NotMasterOrSecondary', :code => 13436},
      ].freeze

      # These are magic error messages that could indicate a master change.
      #
      # @api private
      WRITE_RETRY_MESSAGES = [
        'not master',
        'node is recovering',
      ].freeze

      # These are magic error messages that could indicate a cluster
      # reconfiguration behind a mongos.
      #
      # @api private
      RETRY_MESSAGES = WRITE_RETRY_MESSAGES + [
        'transport error',
        'socket exception',
        "can't connect",
        'connect failed',
        'error querying',
        'could not get last error',
        'connection attempt failed',
        'interrupted at shutdown',
        'unknown replica set',
        'dbclient error communicating with server'
      ].freeze

      # Whether the error is a retryable error according to the legacy
      # read retry logic.
      #
      # @return [ true, false ]
      #
      # @deprecated
      def retryable?
        write_retryable? ||
        code.nil? && RETRY_MESSAGES.any?{ |m| message.include?(m) }
      end

      # Whether the error is a retryable error according to the modern retryable
      # reads and retryable writes specifications.
      #
      # This method is also used by the legacy retryable write logic to determine
      # whether an error is a retryable one.
      #
      # @return [ true, false ]
      def write_retryable?
        write_retryable_code? ||
        code.nil? && WRITE_RETRY_MESSAGES.any? { |m| message.include?(m) }
      end

      private def write_retryable_code?
        if code
          WRITE_RETRY_ERRORS.any? { |e| e[:code] == code }
        else
          # return false rather than nil
          false
        end
      end
    end
  end
end
