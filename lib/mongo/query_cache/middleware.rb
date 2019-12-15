# Copyright (C) 2019 MongoDB, Inc.
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

  module QueryCache

    # The middleware to be added to a rack application in order to activate the
    # query cache.
    #
    # @since 4.0.0
    class Middleware

      # Instantiate the middleware.
      #
      # @example Create the new middleware.
      #   Middleware.new(app)
      #
      # @param [ Object ] app The rack applciation stack.
      #
      # @since 4.0.0
      def initialize(app)
        @app = app
      end

      # Execute the request, wrapping in a query cache.
      #
      # @example Execute the request.
      #   middleware.call(env)
      #
      # @param [ Object ] env The environment.
      #
      # @return [ Object ] The result of the call.
      #
      # @since 4.0.0
      def call(env)
        QueryCache.cache do
          @app.call(env)
        end
      ensure
        QueryCache.clear_cache
      end
    end
  end
end
