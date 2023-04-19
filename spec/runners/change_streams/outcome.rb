# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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
  module ChangeStreams
    class Outcome
      def initialize(spec)
        if spec.nil?
          raise ArgumentError, 'Outcome specification cannot be nil'
        end
        if spec.keys.length != 1
          raise ArgumentError, 'Outcome must have exactly one key: success or error'
        end
        if spec['success']
          @documents = spec['success']
        elsif spec['error']
          @error = spec['error']
        else
          raise ArgumentError, 'Outcome must have exactly one key: success or error'
        end
      end

      attr_reader :documents
      attr_reader :error

      def error?
        !!error
      end
    end
  end
end
