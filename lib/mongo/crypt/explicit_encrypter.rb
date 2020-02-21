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
    #
    # @api private
    module ExplicitEncrypter
      include Encrypter
      # TODO: documentation
      def setup_encrypter(options = {})
        super(options)

        @encryption_io = EncryptionIO.new(key_vault_collection: build_key_vault_collection)
      end
    end
  end
end
