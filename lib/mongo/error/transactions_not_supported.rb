# frozen_string_literal: true

# Copyright (C) 2019-2020 MongoDB Inc.
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
    # Transactions are not supported by the cluster. There might be the
    # following reasons:
    #  - topology is standalone
    #  - topology is replica set and server version is < 4.0
    #  - topology is sharded and server version is < 4.2
    #
    # @param [ String ] reason The reason why transactions are no supported.
    #
    # @since 2.7.0
    class TransactionsNotSupported < Error
      def initialize(reason)
        super("Transactions are not supported for the cluster: #{reason}")
      end
    end
  end
end
