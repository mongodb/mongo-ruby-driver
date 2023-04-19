# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2019-2021 MongoDB Inc.
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
  module Crypt
    module KMS
      module Local
        # Local KMS master key document object contains KMS master key parameters.
        #
        # @api private
        class MasterKeyDocument

          # Creates a master key document object form a parameters hash.
          # This empty method is to keep a uniform interface for all KMS providers.
          def initialize(_opts)
          end

          # Convert master key document object to a BSON document in libmongocrypt format.
          #
          # @return [ BSON::Document ] Local KMS credentials in libmongocrypt format.
          def to_document
            BSON::Document.new({ provider: "local" })
          end
        end
      end
    end
  end
end

