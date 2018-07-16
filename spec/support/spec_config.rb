require 'singleton'

class SpecConfig
  include Singleton

  def initialize
  end

  def mri?
    !jruby?
  end

  def jruby?
    RUBY_PLATFORM =~ /\bjava\b/
  end

  def platform
    RUBY_PLATFORM
  end
end
