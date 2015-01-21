# Copyright (C) 2014-2015 MongoDB, Inc.
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
  module Operation

    # Adds behaviour for commands so ensure the limit option is always -1.
    #
    # @since 2.0.0
    module Limited

      # Limited operations are commands that always require a limit of -1. In
      # these cases we always overwrite the limit value.
      #
      # @example Get the options.
      #   limited.options
      #
      # @return [ Hash ] The options with a -1 limit.
      #
      # @since 2.0.0
      def options
        super.merge(:limit => -1)
      end
    end
  end
end
