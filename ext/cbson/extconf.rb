require 'mkmf'

have_func("asprintf")

have_header("ruby/st.h") || have_header("st.h")
have_header("ruby/regex.h") || have_header("regex.h")
have_header("ruby/encoding.h")

dir_config('cbson')

if "#{RUBY_VERSION}-p#{RUBY_PATCHLEVEL}".eql? '2.0.0-p0'
  FileUtils.mkdir "../bson_ext/bson_ext"
  create_makefile('bson_ext/cbson')  
else
  create_makefile('bson_ext/bson_ext')
end
