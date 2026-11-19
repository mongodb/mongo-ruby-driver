# frozen_string_literal: true

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
      # The value is not included in the message: it may come from the
      # connection string and can contain text the user intended as a
      # credential.
      #
      # @example Instantiate the exception.
      #   Mongo::Error::InvalidReadOption.new({:mode => 'bogus'})
      #
      # @param [ Object ] _read_option The provided read option.
      # @param [ String ] msg Description of the problem.
      #
      # @since 2.6.0
      def initialize(_read_option, msg)
        super("Invalid read preference value: #{msg}")
      end
    end
  end
end
