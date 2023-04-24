# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2019-2020 MongoDB Inc.
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

    # Cache store for computed SCRAM credentials.
    #
    # @api private
    module CredentialCache

      class << self
        attr_reader :store
      end

      module_function def get(key)
        @store ||= {}
        @store[key]
      end

      module_function def set(key, value)
        @store ||= {}
        @store[key] = value
      end

      module_function def cache(key)
        value = get(key)
        if value.nil?
          value = yield
          set(key, value)
        end
        value
      end

      module_function def clear
        @store = {}
      end
    end
  end
end
