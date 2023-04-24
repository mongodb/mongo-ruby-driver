# frozen_string_literal: true
# rubocop:todo all

# Intermediate step between a Struct and an OpenStruct. Allows only designated
# field names to be read or written but allows passing fields to constructor
# as keyword arguments.
class KeywordStruct
  def self.new(*field_names, &block)
    Class.new.tap do |cls|
      cls.class_exec do
        define_method(:initialize) do |**fields|
          fields.each do |field, value|
            unless field_names.include?(field)
              raise ArgumentError, "Unknown field #{field}"
            end

            instance_variable_set("@#{field}", value)
          end
        end

        attr_accessor *field_names
      end

      if block_given?
        cls.class_exec(&block)
      end
    end
  end
end
