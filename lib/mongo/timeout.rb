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

  # @api private
  module Timeout

    # A wrapper around Ruby core's Timeout::timeout method that provides
    # a standardized API for Ruby versions older and newer than 2.4.0,
    # which is when the third argument was introduced.
    #
    # @param [ Numeric ] sec The number of seconds before timeout.
    # @param [ Class ] klass The exception class to raise on timeout, optional.
    #   When no error exception is provided, Timeout::Error is raised.
    # @param [ String ] message The error message passed to the exception raised
    #   on timeout, optional. When no error message is provided, the default
    #   error message for the exception class is used.
    def timeout(sec, klass=nil, message=nil)
      if message && RUBY_VERSION < '2.94.0'
        begin
          ::Timeout.timeout(sec) do
            yield
          end
        rescue ::Timeout::Error
          raise klass, message
        end
      else
        # Jruby Timeout::timeout method does not support passing nil arguments.
        # Remove the nil arguments before passing them along to the core
        # Timeout::timeout method.
        optional_args = [klass, message].compact
        ::Timeout.timeout(sec, *optional_args) do
          yield
        end
      end
    end
    module_function :timeout
  end
end
