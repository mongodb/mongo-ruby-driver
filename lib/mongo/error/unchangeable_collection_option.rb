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
  class Error

    # Raised if a new collection is created from an existing one and options other than the
    # changeable ones are provided.
    #
    # @since 2.1.0
    class UnchangeableCollectionOption < Error

      # Create the new exception.
      #
      # @example Create the new exception.
      #   Mongo::Error::UnchangeableCollectionOption.new(option)
      #
      # @param [ String, Symbol ] option The option that was attempted to be changed.
      #
      # @since 2.1.0
      def initialize(option)
        super("The option #{option} cannot be set on a new collection instance." +
                  " The options that can be updated are #{Collection::CHANGEABLE_OPTIONS}")
      end
    end
  end
end
