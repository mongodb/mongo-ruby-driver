# frozen_string_literal: true
# encoding: utf-8

# Copyright (C) 2015-2020 MongoDB Inc.
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
    class Find
      module Builder

        # Builds a legacy OP_QUERY specification from options.
        #
        # @api private
        module Legacy

          # Mappings from driver options to legacy server values.
          #
          # @since 2.2.0
          DRIVER_MAPPINGS = {
            comment: '$comment',
            explain: '$explain',
            hint: '$hint',
            max_scan: '$maxScan',
            max_time_ms: '$maxTimeMS',
            max_value: '$max',
            min_value: '$min',
            show_disk_loc: '$showDiskLoc',
            snapshot: '$snapshot',
            sort: '$orderby',
            return_key: '$returnKey',
          }.freeze

          module_function def selector(spec, connection)
            if Lint.enabled?
              if spec.keys.any? { |k| String === k }
                raise Error::LintError, "The spec must contain symbol keys only"
              end
            end

            # Server versions that do not have the find command feature
            # (versions older than 3.2) do not support the allow_disk_use option
            # but perform no validation and will not raise an error if it is
            # specified. If the allow_disk_use option is specified, raise an error
            # to alert the user.
            unless spec[:allow_disk_use].nil?
              raise Error::UnsupportedOption.allow_disk_use_error
            end

            if spec[:collation] && !connection.features.collation_enabled?
              raise Error::UnsupportedCollation
            end

            modifiers = {}
            DRIVER_MAPPINGS.each do |k, server_k|
              unless (value = spec[k]).nil?
                modifiers[server_k] = value
              end
            end

            selector = spec[:filter] || BSON::Document.new
            # Write nil into rp if not talking to mongos, rather than false
            rp = if connection.description.mongos?
              read_pref_formatted(spec)
            end
            if modifiers.any? || rp
              selector = {'$query' => selector}.update(modifiers)

              if rp
                selector['$readPreference'] = rp
              end
            end

            selector
          end

          module_function def query_options(spec, connection)
            query_options = {
              project: spec[:projection],
              skip: spec[:skip],
              limit: spec[:limit],
              # batch_size is converted to batchSize by Mongo::Protocol::Query.
              batch_size: spec[:batch_size],
            }

            unless (flags = Builder::Flags.map_flags(spec)).empty?
              query_options[:flags] = ((query_options[:flags] || []) + flags).uniq
            end

            query_options
          end

          private

          module_function def read_pref_formatted(spec)
            if spec[:read_preference]
              raise ArgumentError, "Spec cannot include :read_preference here, use :read"
            end

            if read = spec[:read]
              read_pref = ServerSelector.get(read).to_mongos
              Mongo::Lint.validate_camel_case_read_preference(read_pref)
              read_pref
            else
              nil
            end
          end
        end
      end
    end
  end
end
