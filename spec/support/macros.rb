# frozen_string_literal: true
# rubocop:todo all

module Mongo
  module Macros

    def config_override(key, value)
      around do |example|
        existing = Mongo.send(key)

        Mongo.send("#{key}=", value)

        example.run

        Mongo.send("#{key}=", existing)
      end
    end

    def with_config_values(key, *values, &block)
      values.each do |value|
        context "when #{key} is #{value}" do
          config_override key, value

          class_exec(value, &block)
        end
      end
    end
  end
end
