# Copyright (C) 2009-2014 MongoDB, Inc.
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

  module Fluent

    # Methods for validating update and replacement documents.
    module Validatable

      # Ensure that the document represents a replacement.
      #
      # @todo: document specific error
      # @raise [ Exception ] If the document has keys beginning with '$'.
      #
      # @since 3.0.0
      def validate_replacement!(doc)
        # @todo: update with real error
        raise Exception, "document must not contain any operators" unless doc.keys.all?{|key| key !~ /^\$/}
      end

      # Ensure that the document represents an update.
      #
      # @todo: document specific error
      # @raise [ Exception ] If the first key in the document doesn't begin with '$'.
      #
      # @since 3.0.0
      def validate_update!(doc)
        # @todo: update with real error
        raise Exception, "document must start with an operator" unless !doc.empty? &&
            doc.keys.first.to_s =~ /^\$/
      end
    end
  end
end
