# Copyright (C) 2015-2017 MongoDB, Inc.
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

# Patch for allowing deprecated symbols to be used.
#
# @since 2.2.1
class Symbol

  # Overrides the default BSON type to use the symbol type instead of a
  # string type.
  #
  # @example Get the bson type.
  #   :test.bson_type
  #
  # @return [ String ] The character 14.
  #
  # @since 2.2.1
  def bson_type
    BSON::Symbol::BSON_TYPE
  end
end
