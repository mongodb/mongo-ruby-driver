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

  # These methods uses the FindAndModify command.
  # Each method returns a document, either the original or the modified one.
  module Modifyable

    # Removes a document matching the query spec.
    # The removed document will then be returned.
    #
    # @return [ Hash ] The document that was removed.
    #
    # @raise [ Exception ] If skip has been specified earlier in the chain.
    #
    # @since 3.0.0
    def fetch_one_then_remove
      validate_modify!

      spec = { :findAndModify => collection.name,
               :query         => selector,
               :sort          => sort,
               :remove        => true,
               :new           => false
              }

      Mongo::Operation::Command.new(spec)
    end

    # Replace the document matching the query spec with the provided replacement.
    # The original document will then be returned.
    #
    # @return [ Hash ] The document that has been replaced.
    #
    # @raise [ Exception ] If skip has been specified earlier in the chain.
    # @raise [ Exception ] If the document has keys beginning with '$'.
    #
    # @since 3.0.0
    def fetch_one_then_replace(replacement)
      validate_modify!
      validate_replacement!(replacement)

      spec = { :findAndModify => collection.name,
               :query         => selector,
               :sort          => sort,
               :update        => replacement,
               :new           => false
             }

      Mongo::Operation::Command.new(spec)
    end

    # Update the document matching the query spec by applying the specified update.
    # The original document will then be returned.
    #
    # @return [ Hash ] The original document that has been updated.
    #
    # @raise [ Exception ] If skip has been specified earlier in the chain.
    # @raise [ Exception ] If the first key in the document doesn't begin with '$'.
    #
    # @since 3.0.0
    def fetch_one_then_update(update)
      validate_modify!
      validate_update!(update)

      spec = { :findAndModify => collection.name,
               :query         => selector,
               :sort          => sort,
               :update        => update,
               :new           => false
      }

      Mongo::Operation::Command.new(spec)
    end

    # Replaces the document matching the query spec with the provided replacement.
    # The replaced document is then returned.
    #
    # @return [ Hash ] The replaced document.
    #
    # @raise [ Exception ] If skip has been specified earlier in the chain.
    # @raise [ Exception ] If the document has keys beginning with '$'.
    #
    # @since 3.0.0
    def replace_one_then_fetch(replacement)
      validate_modify!
      validate_replacement!(replacement)

      spec = { :findAndModify => collection.name,
               :query         => selector,
               :sort          => sort,
               :update        => replacement,
               :new           => true
      }

      Mongo::Operation::Command.new(spec)
    end

    # Updates the document matching the query spec with the provided update.
    # The updated document is then returned.
    #
    # @return [ Hash ] The updated document.
    #
    # @raise [ Exception ] If skip has been specified earlier in the chain.
    # @raise [ Exception ] If the first key in the document doesn't begin with '$'.
    #
    # @since 3.0.0
    def update_one_then_fetch(update)
      validate_modify!
      validate_update!(update)

      spec = { :findAndModify => collection.name,
               :query         => selector,
               :sort          => sort,
               :update        => update,
               :new           => true
      }

      Mongo::Operation::Command.new(spec)
    end

    private

    # Verifies that skip has not been specified as it's invalid with these methods.
    #
    # @raise [ Exception ] If skip has been specified earlier in the chain.
    #
    # @since 3.0.0
    def validate_modify!
      # @todo: update with real error
      raise Exception, 'Skip cannot be combined with this method' if skip
    end
  end
end
