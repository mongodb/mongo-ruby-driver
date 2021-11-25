# frozen_string_literal: true
# encoding: utf-8

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
        class Credentials
          include KMS::Validations

          attr_reader :key

          FORMAT_HINT = "Local KMS provider options must be in the format: " +
                        "{ key: 'MASTER-KEY' }"

          def initialize(opts)
            @key = validate_param(:key, opts, FORMAT_HINT)
          end

          # @return [ BSON::Document ] Azure KMS credentials in libmongocrypt format.
          def to_document
            BSON::Document.new({
              key: BSON::Binary.new(@key, :generic),
            })
          end
        end

        class KeyDocument
          def initialize(opts)
          end

          # @return [ BSON::Document ] Local KMS credentials in libmongocrypt format.
          def to_document
            BSON::Document.new({ provider: "local" })
          end
        end
      end
    end
  end
end
