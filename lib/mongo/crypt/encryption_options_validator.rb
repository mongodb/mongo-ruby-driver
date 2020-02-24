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
  module Crypt
    # TODO: documentation
    module EncrypterHelper

      private def key_vault_collection
        key_vault_namespace = @options[:key_vault_namespace]
        key_vault_client = @options[:key_vault_client]

        unless key_vault_namespace
          raise ArgumentError.new('The :key_vault_namespace option cannot be nil')
        end

        unless key_vault_namespace.split('.').length == 2
          raise ArgumentError.new(
            "#{key_vault_namespace} is an invalid key vault namespace." +
            "The :key_vault_namespace option must be in the format database.collection"
          )
        end

        unless key_vault_client
          raise ArgumentError.new('The :key_vault_client option cannot be nil')
        end

        unless key_vault_client.is_a?(Client)
          raise ArgumentError.new(
            'The :key_vault_client option must be an instance of Mongo::Client'
          )
        end

        key_vault_db, key_vault_coll = key_vault_namespace.split('.')
        key_vault_client.use(key_vault_db)[key_vault_coll]
      end
    end
  end
end
