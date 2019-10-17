# Copyright (C) 2019 MongoDB, Inc.
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

require 'ffi'

module Mongo
  class Libmongocrypt

    # A Ruby binding for the libmongocrypt C library
    #
    # @since 2.12.0
    class Binding
      extend FFI::Library

      unless ENV['LIBMONGOCRYPT_PATH']
        raise "Cannot load Mongo::Libmongocrypt::Binding because there is no path " +
            "to libmongocrypt specified in the LIBMONGOCRYPT_PATH environment variable."
      end

      begin
        ffi_lib ENV['LIBMONGOCRYPT_PATH']
      rescue LoadError => e
        raise "Cannot load Mongo::Libmongocrypt::Binding because the path to " +
          "libmongocrypt specified in the LIBMONGOCRYPT_PATH environment variable " +
          "is invalid: #{ENV['LIBMONGOCRYPT']}\n\n#{e.class}: #{e.message}"
      end

      attach_function :mongocrypt_version, [:pointer], :string
    end
  end
end
