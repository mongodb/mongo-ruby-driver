# Copyright (C) 2009-2014 MongoDB, Inc.
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
  class Server

    # Represents what kind of type a server is.
    #
    # @since 2.0.0
    class Type

      # Constant for arbiter types.
      #
      # @since 2.0.0
      ARBITER = :arbiter.freeze

      # Constant for ghost types.
      #
      # @since 2.0.0
      GHOST = :ghost.freeze

      # Constant for mongos types.
      #
      # @since 2.0.0
      MONGOS = :mongos.freeze

      # Constant for other types.
      #
      # @since 2.0.0
      OTHER = :other.freeze

      # Constant for primary types.
      #
      # @since 2.0.0
      PRIMARY = :primary.freeze

      # Constant for secondary types.
      #
      # @since 2.0.0
      SECONDARY = :secondary.freeze

      # Constant for standalone types.
      #
      # @since 2.0.0
      STANDALONE = :standalone.freeze

      # Constant for unknown types.
      #
      # @since 2.0.0
      UNKNOWN = :unknown.freeze

      # List of rules for determining the type of server it is. We have this
      # logic here since we don't determine whether a server should be
      # selectable by the cluster at the time a message is to be dispatched,
      # but at the time the description is updated. This means we need to store
      # a server type and determine if it changed. This is much cleaner then
      # checking each predicate method on both objects every time - this way it
      # only happens on the new description.
      #
      # @since 2.0.0
      RULES = [
        ->(description){ description.arbiter?   ? ARBITER   : false },
        ->(description){ description.ghost?     ? GHOST     : false },
        ->(description){ description.mongos?    ? MONGOS    : false },
        ->(description){ description.primary?   ? PRIMARY   : false },
        ->(description){ description.secondary? ? SECONDARY : false },
        ->(description){ description.unknown?   ? UNKNOWN   : false }
      ]

      class << self

        # Determine the server type given the description.
        #
        # @example Determine the server type.
        #   Type.determine(description)
        #
        # @param [ Description ] description The server description.
        #
        # @return [ Symbol ] The server type.
        #
        # @since 2.0.0
        def determine(description)
          RULES.each do |rule|
            type = rule.call(description)
            return type if type
          end
        end
      end
    end
  end
end
