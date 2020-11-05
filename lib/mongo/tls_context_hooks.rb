# Copyright (C) 2020 MongoDB, Inc.
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
  # This module maintains a user-settable list of hooks that will be invoked
  # when any new TLS socket is connected. Each hook should be a Proc that takes
  # an OpenSSL::SSL::SSLContext object as an argument. These hooks can be used
  # to modify the TLS context (for example to disallow certain ciphers).
  module TLSContextHooks
    class << self
      # Return the list of TLS hooks.
      #
      # @return [ Array<Proc> ] The list of procs to be invoked when a TLS
      #   socket is connected (or an empty Array).
      def hooks
        @hooks ||= []
      end

      # Set the TLS context hooks.
      #
      # @param [ Array<Proc> ] hooks An Array of Procs, each of which should take
      #   an OpenSSL::SSL::SSLContext object as an argument.
      def hooks=(hooks)
        unless hooks.is_a?(Array) && hooks.all? { |hook| hook.is_a?(Proc) }
          raise ArgumentError, "TLS context hooks must be an array of Procs"
        end

        @hooks=hooks
      end

      # Clear the TLS context hooks.
      def clear_hooks
        @hooks = []
      end
    end
  end
end
