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

    # Raised when the driver is used incorrectly.
    #
    # Normally the driver passes certain data to the server and lets the
    # server return an error if the data is invalid. This makes it possible
    # for the server to add functionality in the future and for older
    # driver versions to support such functionality transparently, but
    # also complicates debugging.
    #
    # Setting the environment variable MONGO_RUBY_DRIVER_LINT to 1, true
    # or yes will make the driver perform additional checks on data it passes
    # to the server, to flag failures sooner. This exception is raised on
    # such failures.
    #
    # @since 2.6.1
    class LintError < Error
    end
  end
end
