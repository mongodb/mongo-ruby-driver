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

require 'securerandom'
require 'digest'

module Mongo
  module Crypt

    # A helper module that implements cryptography methods required
    # for native Ruby crypto hooks. These methods are passed into FFI
    # as C callbacks and called from the libmongocrypt library.
    #
    # @api private
    module Hooks

      # An AES encrypt or decrypt method.
      #
      # @param [ String ] key The 32-byte AES encryption key
      # @param [ String ] iv The 16-byte AES IV
      # @param [ String ] input The data to be encrypted/decrypted
      # @param [ true | false ] decrypt Whether this method is decrypting. Default is
      #   false, which means the method will create an encryption cipher by default
      #
      # @return [ String ] Output
      # @raise [ Exception ] Exceptions raised during encryption are propagated
      #   to caller.
      def aes(key, iv, input, decrypt: false)
        cipher = OpenSSL::Cipher::AES.new(256, :CBC)

        decrypt ? cipher.decrypt : cipher.encrypt
        cipher.key = key
        cipher.iv = iv
        cipher.padding = 0

        encrypted = cipher.update(input)
      end
      module_function :aes

      # Crypto secure random function
      #
      # @param [ Integer ] num_bytes The number of random bytes requested
      #
      # @return [ String ]
      # @raise [ Exception ] Exceptions raised during encryption are propagated
      #   to caller.
      def random(num_bytes)
        SecureRandom.random_bytes(num_bytes)
      end
      module_function :random

      # An HMAC SHA-512 or SHA-256 function
      #
      # @param [ String ] digest_name The name of the digest, either "SHA256" or "SHA512"
      # @param [ String ] key The 32-byte AES encryption key
      # @param [ String ] input The data to be tagged
      #
      # @return [ String ]
      # @raise [ Exception ] Exceptions raised during encryption are propagated
      #   to caller.
      def hmac_sha(digest_name, key, input)
        OpenSSL::HMAC.digest(digest_name, key, input)
      end
      module_function :hmac_sha

      # A crypto hash (SHA-256) function
      #
      # @param [ String ] input The data to be hashed
      #
      # @return [ String ]
      # @raise [ Exception ] Exceptions raised during encryption are propagated
      #   to caller.
      def hash_sha256(input)
        Digest::SHA2.new(256).digest(input)
      end
      module_function :hash_sha256
    end
  end
end
