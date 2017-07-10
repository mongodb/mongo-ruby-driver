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

module Mongo
  class BulkWrite

    # Defines behaviour around validations.
    #
    # @api private
    #
    # @since 2.1.0
    module Validatable

      # Validate the document.
      #
      # @api private
      #
      # @example Validate the document.
      #   validatable.validate(:insert_one, { _id: 0 })
      #
      # @param [ Symbol ] name The operation name.
      # @param [ Hash, BSON::Document ] document The document.
      #
      # @raise [ InvalidBulkOperation ] If not valid.
      #
      # @return [ Hash, BSON::Document ] The document.
      #
      # @since 2.1.0
      def validate(name, document)
        validate_operation(name)
        validate_document(name, document)
        if document.respond_to?(:keys) && (document[:collation] || document[Operation::COLLATION])
          @has_collation = true
        end
      end

      private

      def validate_document(name, document)
        if document.respond_to?(:keys) || document.respond_to?(:data)
          document
        else
          raise Error::InvalidBulkOperation.new(name, document)
        end
      end

      def validate_operation(name)
        unless Transformable::MAPPERS.key?(name)
          raise Error::InvalidBulkOperationType.new(name)
        end
      end
    end
  end
end
