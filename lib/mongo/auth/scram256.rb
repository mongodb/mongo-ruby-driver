# frozen_string_literal: true
# rubocop:todo all

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
  module Auth

    # Defines behavior for SCRAM-SHA-256 authentication.
    #
    # The purpose of this class is to provide the namespace for the
    # Scram256::Conversation class.
    #
    # @api private
    class Scram256 < Scram
      # The authentication mechanism string.
      MECHANISM = 'SCRAM-SHA-256'.freeze
    end
  end
end

require 'mongo/auth/scram256/conversation'
