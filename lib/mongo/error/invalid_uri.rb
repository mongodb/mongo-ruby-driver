# Copyright (C) 2014-2015 MongoDB, Inc.
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

    # Exception that is raised when trying to parse a URI that does not match
    # the specification.
    #
    # @since 2.0.0
    class InvalidURI < Error

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::Error::InvalidURI.new(uri)
      #
      # @since 2.0.0
      def initialize(uri, details)
        super("Bad URI: #{uri}\n" +
        "#{details}\n" +
        "MongoDB URI must be in the following format: #{Mongo::URI::FORMAT}\n" +
        "Please see the following URL for more information: #{Mongo::URI::HELP}\n")
      end
    end
  end
end
