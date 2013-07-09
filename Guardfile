guard 'rspec', :all_after_pass => false do
  watch(%r{^spec/.+_spec\.rb$})
  watch(%r{^lib/(.+)\.rb$}) { |match| 'spec/#{match[1]}_spec.rb' }
  watch('spec/spec_helper.rb')  { 'spec' }
end

unless RUBY_VERSION < '1.9'
  guard :rubocop do
    watch(%r{.+\.rb$})
    watch(%r{(?:.+/)?\.rubocop\.yml$}) { |m| File.dirname(m[0]) }
  end
end
