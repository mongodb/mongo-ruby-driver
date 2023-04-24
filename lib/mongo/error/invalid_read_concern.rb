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
  class Error

    # Raised when an invalid read concern is provided.
    class InvalidReadConcern < Error
      # Instantiate the new exception.
      def initialize(msg = nil)
        super(msg || 'Invalid read concern option provided.' \
              'The only valid key is :level, for which accepted values are' \
              ':local, :majority, and :snapshot')
      end
    end
  end
end
