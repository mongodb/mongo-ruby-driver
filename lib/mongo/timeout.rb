# Copyright (C) 2020 MongoDB, Inc.
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
  module Timeout

    # A wrapper around Ruby core's Timeout#timeout method that provides
    # a standardized API for Ruby versions older and newer than 2.4.0,
    # which is when the third argument was introduced.
    #
    # @param [ Numeric ] sec The number of seconds before timeout.
    # @param [ Class ] klass The error class to raise on timeout, optional.
    #   When no error class is provided, StandardError is raised.
    # @param [ String ] message The error message passed to the error raised
    #   on timeout, optional. When no error message is provided, the default
    #   error message is "execution expired".
    #
    # @note Ruby versions older than 2.4.0 do not support specifying a custom
    #   error message, and any error message passed in as an argument will be
    #   ignored.
    def timeout(sec, klass=nil, message=nil)
      if RUBY_VERSION < '2.4.0'
        ::Timeout.timeout(sec, klass) do
          yield
        end
      else
        ::Timeout.timeout(sec, klass, message) do
          yield
        end
      end
    end
    module_function :timeout
  end
end
