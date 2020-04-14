# Copyright (C) 2018-2020 MongoDB Inc.
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

    # Custom behavior for operations that support the bypassdocumentvalidation option.
    #
    # @since 2.5.2
    module BypassDocumentValidation

      private

      def command(connection)
        sel = super
        add_bypass_document_validation(sel)
      end

      def add_bypass_document_validation(sel)
        return sel unless bypass_document_validation
        sel.merge(bypassDocumentValidation: true)
      end
    end
  end
end
