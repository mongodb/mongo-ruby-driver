# frozen_string_literal: true
# encoding: utf-8

# Copyright (C) 2014-2022 MongoDB Inc.
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
  class Collection
    module IndexedEncryptedFields

      def maybe_create_emm_collections(encrypted_fields, client, session)
        encrypted_fields = if encrypted_fields
          encrypted_fields
        elsif encrypted_fields_map
          encrypted_fields_map[namespace] || {}
        else
          {}
        end
        if encrypted_fields.empty?
          return yield
        end

        emm_collections(encrypted_fields).each do |coll|
          context = Operation::Context.new(client: client, session: session)
          Operation::Create.new(
            selector: {
              create: coll,
              clusteredIndex: {
                key: {_id: 1}, unique: true
              }
            },
            db_name: database.name,
          ).execute(next_primary(nil, session), context: context)
        end
        yield(encrypted_fields).tap do |result|
          indexes.create_one("__safeContent__" => 1) if result
        end
      end

      def maybe_drop_emm_collections(encrypted_fields, client, session)
        encrypted_fields = if encrypted_fields
          encrypted_fields
        elsif encrypted_fields_map
          if encrypted_fields_map[namespace]
            encrypted_fields_map[namespace]
          else
            database.list_collections(filter: { name: name }).first&.fetch(:options, {})&.fetch(:encryptedFields, {}) || {}
          end
        else
          {}
        end
        if encrypted_fields.empty?
          return yield
        end
        emm_collections(encrypted_fields).each do |coll|
          begin
            context = Operation::Context.new(client: client, session: session)
            Operation::Drop.new(
              selector: { drop: coll },
              db_name: database.name,
              session: session,
            ).execute(next_primary(nil, session), context: context)
          rescue Error::OperationFailure => ex
            # NamespaceNotFound
            unless ex.code == 26 || ex.code.nil? && ex.message =~ /ns not found/
              raise
            end
          end
        end
        yield
      end

      private

      def emm_collections(encrypted_fields)
        [
          encrypted_fields['escCollection'] || "enxcol_.#{name}.esc",
          encrypted_fields['eccCollection'] || "enxcol_.#{name}.ecc",
          encrypted_fields['ecocCollection'] || "enxcol_.#{name}.ecoc",
        ]
      end

    end
  end
end
