# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2023-present MongoDB Inc.
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
    class Aws
      # Thread safe cache to store AWS credentials.
      #
      # @api private
      class CredentialsCache
        # Get or create the singleton instance of the cache.
        #
        # @return [ CredentialsCache ] The singleton instance.
        def self.instance
          @instance ||= new
        end

        def initialize
          @lock = Mutex.new
          @credentials = nil
        end

        # Set the credentials in the cache.
        #
        # @param [ Aws::Credentials ] credentials The credentials to cache.
        def credentials=(credentials)
          @lock.synchronize do
            @credentials = credentials
          end
        end

        # Get the credentials from the cache.
        #
        # @return [ Aws::Credentials ] The cached credentials.
        def credentials
          @lock.synchronize do
            @credentials
          end
        end

        # Fetch the credentials from the cache or yield to get them
        # if they are not in the cache or have expired.
        #
        # @return [ Aws::Credentials ] The cached credentials.
        def fetch
          @lock.synchronize do
            @credentials = yield if @credentials.nil? || @credentials.expired?
            @credentials
          end
        end

        # Clear the credentials from the cache.
        def clear
          @lock.synchronize do
            @credentials = nil
          end
        end
      end
    end
  end
end
