# frozen_string_literal: true
# encoding: utf-8

# Copyright (C) 2015-2020 MongoDB Inc.
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

require 'mongo/operation/update/command'
require 'mongo/operation/update/op_msg'
require 'mongo/operation/update/result'
require 'mongo/operation/update/bulk_result'

module Mongo
  module Operation

    # A MongoDB update operation.
    #
    # @api private
    #
    # @since 2.0.0
    class Update
      include Specifiable
      include Write

      def initialize(spec)
        validate_update_documents!(spec[:updates])
        super
      end

      private

      def validate_update_documents!(updates)
        if update = updates.first
          # As per the spec, we only have to examine the first element in the
          # update document.
          if key = update["u"]&.keys&.first
            unless key.start_with?("$")
              raise Error::InvalidUpdateDocument.new(key)
            end
          end
        end
      end

      IDENTIFIER = 'updates'.freeze
    end
  end
end
