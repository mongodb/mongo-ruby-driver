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
  class Error

    # Exception that is raised when trying to create a client with an invalid
    #   read option.
    #
    # @since 2.6.0
    class InvalidReadOption < Error

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::Error::InvalidReadOption.new({:mode => 'bogus'})
      #
      # @since 2.6.0
      def initialize(read_option, msg)
        super("Invalid read preference value: #{read_option.inspect}: #{msg}")
      end
    end
  end
end
