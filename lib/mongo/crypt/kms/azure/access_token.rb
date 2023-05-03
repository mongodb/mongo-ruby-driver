# frozen_string_literal: true

# Copyright (C) 2019-2021 MongoDB Inc.
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
  module Crypt
    module KMS
      module Azure
        # Azure access token for temporary credentials.
        #
        # @api private
        class AccessToken
          # @return [ String ] Azure access token.
          attr_reader :access_token

          # @return [ Integer ] Azure access token expiration time.
          attr_reader :expires_in

          # Creates an Azure access token object.
          #
          # @param [ String ] access_token Azure access token.
          # @param [ Integer ] expires_in Azure access token expiration time.
          def initialize(access_token, expires_in)
            @access_token = access_token
            @expires_in = expires_in
            @expires_at = Time.now.to_i + @expires_in
          end

          # Checks if the access token is expired.
          #
          # The access token is considered expired if it is within 60 seconds
          # of its expiration time.
          #
          # @return [ true | false ] Whether the access token is expired.
          def expired?
            Time.now.to_i >= @expires_at - 60
          end
        end
      end
    end
  end
end
