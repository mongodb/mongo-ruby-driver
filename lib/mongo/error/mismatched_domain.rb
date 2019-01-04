# Copyright (C) 2017-2019 MongoDB, Inc.
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

    # This exception is raised when the URI Parser's DNS query returns SRV record(s)
    #   whose parent domain does not match the hostname used for the query.
    #
    # @example Instantiate the exception.
    #   Mongo::Error::MismatchedDomain.new(message)
    #
    # @since 2.5.0
    class MismatchedDomain < Error; end
  end
end
