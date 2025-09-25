autoload :YAML, 'yaml'
require 'erubi'
require 'erubi/capture_end'
require 'tilt'

module Mrss
  module EgConfigUtils

    DEBIAN_FOR_RUBY = {
      'ruby-2.3' => 'debian92',
      'ruby-2.4' => 'debian92',
      'ruby-2.5' => 'debian10',
      'ruby-2.6' => 'debian10',
      'ruby-2.7' => 'debian10',
      'ruby-3.0' => 'debian10',
    }

    def standard_debian_rubies(rubies, key: nil, &block)
      rubies.flatten!
      text = block.call
      contents = YAML.load(text)
      out = rubies.map do |ruby|
        contents.merge(
          'matrix_name' => "#{contents['matrix_name']} - #{ruby}",
          'matrix_spec' => contents['matrix_spec'].merge(
            'ruby' => ruby,
            key || 'os' => DEBIAN_FOR_RUBY.fetch(ruby),
          ),
        )
      end.to_yaml
      text =~ /\A\n?(\s+)/
      unless text
        raise "Couldn't figure out indentation level"
      end
      indent = ' ' * ($1.length - 2)
      "\n" + out.sub(/\A---.*\n/, indent).gsub("\n", "\n#{indent}")
    end

    def transform_config(template_path, context)
      Tilt.new(template_path, engine_class: Erubi::CaptureEndEngine).render(context)
    end

    def generated_file_warning
      <<-EOT
# GENERATED FILE - DO NOT EDIT.
# Run ./.evergreen/update-evergreen-configs to regenerate this file.

EOT
    end
  end
end
