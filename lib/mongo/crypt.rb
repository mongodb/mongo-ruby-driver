# frozen_string_literal: true
# encoding: utf-8

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
  module Crypt
    autoload(:Binding, 'mongo/crypt/binding')
    autoload(:Binary, 'mongo/crypt/binary')
    autoload(:Status, 'mongo/crypt/status')
    autoload(:Hooks, 'mongo/crypt/hooks')
    autoload(:Handle, 'mongo/crypt/handle')
    autoload(:KmsContext, 'mongo/crypt/kms_context')
    autoload(:Context, 'mongo/crypt/context')
    autoload(:DataKeyContext, 'mongo/crypt/data_key_context')
    autoload(:ExplicitEncryptionContext, 'mongo/crypt/explicit_encryption_context')
    autoload(:AutoEncryptionContext, 'mongo/crypt/auto_encryption_context')
    autoload(:ExplicitDecryptionContext, 'mongo/crypt/explicit_decryption_context')
    autoload(:AutoDecryptionContext, 'mongo/crypt/auto_decryption_context')
    autoload(:RewrapManyDataKeyContext, 'mongo/crypt/rewrap_many_data_key_context')
    autoload(:RewrapManyDataKeyResult, 'mongo/crypt/rewrap_many_data_key_result')
    autoload(:EncryptionIO, 'mongo/crypt/encryption_io')
    autoload(:ExplicitEncrypter, 'mongo/crypt/explicit_encrypter')
    autoload(:AutoEncrypter, 'mongo/crypt/auto_encrypter')
    autoload(:KMS, 'mongo/crypt/kms')
  end
end
