module Mongo
  module ReadPreference
    READ_PREFERENCES = [
      :primary,
      :primary_preferred,
      :secondary,
      :secondary_preferred,
      :nearest
    ]

    def self.validate(value)
      if READ_PREFERENCES.include?(value)
        return true
      else
        raise MongoArgumentError, "#{value} is not a valid read preference. " +
          "Please specify one of the following read preferences as a symbol: #{READ_PREFERENCES}"
      end
    end
  end
end