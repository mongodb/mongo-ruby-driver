# frozen_string_literal: true
# rubocop:todo all

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

require 'mongo/auth/stringprep/tables'
require 'mongo/auth/stringprep/profiles/sasl'

module Mongo
  module Auth
    # This namespace contains all behavior related to string preparation
    # (RFC 3454). It's used to implement SCRAM-SHA-256 authentication,
    # which is available in MongoDB server versions 4.0 and later.
    #
    # @since 2.6.0
    # @api private
    module StringPrep
      extend self

      # Prepare a string given a set of mappings and prohibited character tables.
      #
      # @example Prepare a string.
      #   StringPrep.prepare("some string",
      #                      StringPrep::Profiles::SASL::MAPPINGS,
      #                      StringPrep::Profiles::SASL::PROHIBITED,
      #                      normalize: true, bidi: true)
      #
      # @param [ String ] data The string to prepare.
      # @param [ Array ] mappings A list of mappings to apply to the data.
      # @param [ Array ] prohibited A list of prohibited character lists to ensure the data doesn't
      #   contain after mapping and normalizing the data.
      # @param [ Hash ] options Optional operations to perform during string preparation.
      #
      # @option options [ Boolean ] :normalize Whether or not to apply Unicode normalization to the
      #   data.
      # @option options [ Boolean ] :bidi Whether or not to ensure that the data contains valid
      #   bidirectional input.
      #
      # @raise [ Error::FailedStringPrepValidation ] If stringprep validations fails.
      #
      # @since 2.6.0
      def prepare(data, mappings, prohibited, options = {})
        apply_maps(data, mappings).tap do |mapped|
          normalize!(mapped) if options[:normalize]
          check_prohibited!(mapped, prohibited)
          check_bidi!(mapped) if options[:bidi]
        end
      end

      private

      def apply_maps(data, mappings)
        data.each_char.inject(+'') do |out, c|
          out << mapping(c.ord, mappings)
        end
      end

      def check_bidi!(out)
        if out.each_char.any? { |c| table_contains?(Tables::C8, c) }
          raise Mongo::Error::FailedStringPrepValidation.new(Error::FailedStringPrepValidation::INVALID_BIDIRECTIONAL)
        end

        if out.each_char.any? { |c| table_contains?(Tables::D1, c) }
          if out.each_char.any? { |c| table_contains?(Tables::D2, c) }
            raise Mongo::Error::FailedStringPrepValidation.new(Error::FailedStringPrepValidation::INVALID_BIDIRECTIONAL)
          end

          unless table_contains?(Tables::D1, out[0]) && table_contains?(Tables::D1, out[-1])
            raise Mongo::Error::FailedStringPrepValidation.new(Error::FailedStringPrepValidation::INVALID_BIDIRECTIONAL)
          end
        end
      end

      def check_prohibited!(out, prohibited)
        out.each_char do |c|
          prohibited.each do |table|
            if table_contains?(table, c)
              raise Error::FailedStringPrepValidation.new(Error::FailedStringPrepValidation::PROHIBITED_CHARACTER)
            end
          end
        end
      end

      def mapping(c, mappings)
        m = mappings.find { |m| m.has_key?(c) }
        mapped = (m && m[c]) || [c]
        mapped.map { |i| i.chr(Encoding::UTF_8) }.join
      end

      def normalize!(out)
        if String.method_defined?(:unicode_normalize!)
          out.unicode_normalize!(:nfkc)
        else
          require 'mongo/auth/stringprep/unicode_normalize/normalize'
          out.replace(UnicodeNormalize.normalize(out, :nfkc))
        end
      end

      def table_contains?(table, c)
        table.any? do |r|
          r.member?(c.ord)
        end
      end
    end
  end
end
