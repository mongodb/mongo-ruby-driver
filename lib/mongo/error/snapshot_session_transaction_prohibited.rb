# frozen_string_literal: true
# encoding: utf-8

# Copyright (C) 2021 MongoDB Inc.
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

    # Exception raised if a transaction is attempted on a snapshot session.
    class SnapshotSessionTransactionProhibited < Error

      # Instantiate the new exception.
      def initialize
        super("Transactions are not supported in snapshot sessions")
      end
    end
  end
end
