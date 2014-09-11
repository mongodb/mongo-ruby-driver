require 'mkmf'
find_header('sasl/sasl.h')
have_library('sasl2', 'sasl_version')

create_makefile('csasl/csasl')
