# frozen_string_literal: true

# Intermediate step between a Struct and an OpenStruct. Allows only designated
# field names to be read or written but allows passing fields to constructor
# as keyword arguments.
class KeywordStruct
  def self.new(*field_names, &block)
    Class.new.tap do |cls|
      cls.class_exec do
        define_method(:initialize) do |**fields|
          fields.each do |field, value|
            raise ArgumentError, "Unknown field #{field}" unless field_names.include?(field)

            instance_variable_set("@#{field}", value)
          end
        end

        attr_accessor(*field_names)
      end

      cls.class_exec(&block) if block_given?
    end
  end
end
