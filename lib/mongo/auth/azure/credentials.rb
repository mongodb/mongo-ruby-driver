# frozen_string_literal: true
# encoding: utf-8

# Copyright (C) 2022 MongoDB Inc.
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
    module Azure
      # @api private
      class Credentials

        attr_reader :access_token

        attr_reader :resource

        attr_reader :token_type

        MINUTE = 60

        def initialize(access_token:, resource:, token_type:, expires_in:)
          @access_token = access_token
          @resource = resource
          @token_type = token_type
          @expires_in = Integer(expires_in)
          @expires_at = Time.now + @expires_in
        end

        def valid?
          (@expires_at - Time.now).floor > MINUTE
        end

        def to_h
          {
            'accessToken' => @access_token
          }
        end
      end
    end
  end
end

