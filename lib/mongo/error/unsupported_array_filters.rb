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

    # Raised if the array filters option is specified for an operation but the server
    # selected does not support array filters.
    #
    # @since 2.5.0
    #
    # @deprecated RUBY-2260 In driver version 3.0, this error class will be 
    #   replaced with UnsupportedOption. To handle this error, catch
    #   Mongo::Error::UnsupportedOption, which will prevent any breaking changes
    #   in your application when upgrading to version 3.0 of the driver.
    class UnsupportedArrayFilters < UnsupportedOption

      # The default error message describing that array filters are not supported.
      #
      # @return [ String ] A default message describing that array filters are not supported by the server.
      #
      # @since 2.5.0
      DEFAULT_MESSAGE = "The array_filters option is not a supported feature of the server handling this operation. " +
          "Operation results may be unexpected.".freeze

      # The error message describing that array filters cannot be used when write concern is unacknowledged.
      #
      # @return [ String ] A message describing that array filters cannot be used when write concern is unacknowledged.
      #
      # @since 2.5.0
      UNACKNOWLEDGED_WRITES_MESSAGE = "The array_filters option cannot be specified when using unacknowledged writes. " +
        "Either remove the array_filters option or use acknowledged writes (w >= 1).".freeze

      # Create the new exception.
      #
      # @example Create the new exception.
      #   Mongo::Error::UnsupportedArrayFilters.new
      #
      # @since 2.5.0
      def initialize(message = nil)
        super(message || DEFAULT_MESSAGE)
      end
    end
  end
end
