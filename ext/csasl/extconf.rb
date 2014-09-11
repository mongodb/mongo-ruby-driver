require 'mkmf'
find_header('sasl/sasl.h')

if have_library('sasl2', 'sasl_version')
  create_makefile('csasl/csasl')
else
  dummy_makefile('csasl/csasl')
end
