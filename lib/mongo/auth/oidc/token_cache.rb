# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2024 MongoDB Inc.
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
  module Auth
    class Oidc
      # Represents a cache of the OIDC access token.
      class TokenCache
        attr_accessor :access_token
        attr_reader :lock

        def initialize
          @lock = Mutex.new
        end

        # Is there an access token present in the cache?
        #
        # @returns [ Boolean ] True if present, false if not.
        def access_token?
          !!@access_token
        end

        # Invalidate the token. Will only invalidate if the token
        # matches the existing one and only one thread at a time
        # may invalidate the token.
        #
        # @params [ String ] token The access token to invalidate.
        def invalidate(token:)
          lock.synchronize do
            if (access_token == token)
              @access_token = nil
            end
          end
        end
      end
    end
  end
end