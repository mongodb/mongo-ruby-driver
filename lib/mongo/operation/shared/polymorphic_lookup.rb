# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2018-2020 MongoDB Inc.
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
  module Operation

    # Shared behavior of looking up a class based on the name of
    # the receiver's class.
    #
    # @api private
    module PolymorphicLookup
      private

      def polymorphic_class(base, name)
        bits = (base + "::#{name}").split('::')
        bits.reduce(Object) do |cls, name|
          cls.const_get(name, false)
        end
      end
    end
  end
end
