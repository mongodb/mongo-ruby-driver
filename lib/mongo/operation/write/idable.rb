# Copyright (C) 2014-2017 MongoDB, Inc.
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
    module Write

      # This module provides functionality to ensure that documents contain
      # an id field. Used by insert operations (Bulk, legacy, write command inserts).
      #
      # @since 2.1.0
      module Idable

        # The option for a custom id generator.
        #
        # @since 2.2.0
        ID_GENERATOR = :id_generator.freeze

        # Get the id generator.
        #
        # @example Get the id generator.
        #   idable.id_generator
        #
        # @return [ IdGenerator ] The default or custom id generator.
        #
        # @since 2.2.0
        def id_generator
          @id_generator ||= (spec[ID_GENERATOR] || ObjectIdGenerator.new)
        end

        private

        def id(doc)
          doc.respond_to?(:id) ? doc.id : (doc['_id'] || doc[:_id])
        end

        def has_id?(doc)
          !!id(doc)
        end

        def ensure_ids(documents)
          @ids ||= []
          documents.collect do |doc|
            doc_with_id = has_id?(doc) ? doc : doc.merge(_id: id_generator.generate)
            @ids << id(doc_with_id)
            doc_with_id
          end
        end
      end
    end
  end
end
