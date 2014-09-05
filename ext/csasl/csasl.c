// Copyright (C) 2014 MongoDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <ruby.h>
#include <sasl/sasl.h>
#include <sasl/saslutil.h>

static void mongo_sasl_conn_free(void* data) {
  sasl_conn_t *conn = (sasl_conn_t*) data;
  // Ideally we would use sasl_client_done() but that's only available as of cyrus sasl 2.1.25
  if(conn) sasl_done();
}

static sasl_conn_t* mongo_sasl_context(VALUE self) {
  sasl_conn_t* conn;
  VALUE context;
  context = rb_iv_get(self, "@context");
  Data_Get_Struct(context, sasl_conn_t, conn);
  return conn;
}

static VALUE a_init(VALUE self, VALUE user_name, VALUE host_name, VALUE service_name, VALUE canonicalize_host_name)
{
  if (sasl_client_init(NULL) == SASL_OK) {
    rb_iv_set(self, "@valid", Qtrue);
    rb_iv_set(self, "@user_name", user_name);
    rb_iv_set(self, "@host_name", host_name);
    rb_iv_set(self, "@service_name", service_name);
    rb_iv_set(self, "@canonicalize_host_name", canonicalize_host_name);
  }

  else {
    rb_iv_set(self, "@valid", Qfalse);
  }

  return self;
}

static VALUE valid(VALUE self) {
  return rb_iv_get(self, "@valid");
}

int is_sasl_failure(int result)
{
  if (result < 0) {
    return 1;
  }

  return 0;
}

static int sasl_interact(VALUE self, int id, const char **result, unsigned *len) {
  switch (id) {
    case SASL_CB_AUTHNAME:
    case SASL_CB_USER:
    {
      VALUE user_name;
      user_name = rb_iv_get(self, "@user_name");
      *result = RSTRING_PTR(user_name);
      if (len) {
        *len = RSTRING_LEN(user_name);
      }
      return SASL_OK;
    }
  }

  return SASL_FAIL;
}

static VALUE initialize_challenge(VALUE self) {
  int result;
  char encoded_payload[4096];
  const char *raw_payload;
  unsigned int raw_payload_len, encoded_payload_len;
  const char *mechanism_list = "GSSAPI";
  const char *mechanism_selected = "GSSAPI";
  VALUE context;
  sasl_conn_t *conn;
  sasl_callback_t client_interact [] = {
    { SASL_CB_AUTHNAME, (int (*)(void))sasl_interact, (void*)self },
    { SASL_CB_USER, (int (*)(void))sasl_interact, (void*)self },
    { SASL_CB_LIST_END, NULL, NULL }
  };

  const char *servicename = RSTRING_PTR(rb_iv_get(self, "@service_name"));
  const char *hostname = RSTRING_PTR(rb_iv_get(self, "@host_name"));

  result = sasl_client_new(servicename, hostname, NULL, NULL, client_interact, 0, &conn);

  if (result != SASL_OK) {
    sasl_dispose(&conn);
    return Qfalse;
  }

  context = Data_Wrap_Struct(rb_cObject, NULL, mongo_sasl_conn_free, conn);
  rb_iv_set(self, "@context", context);

  result = sasl_client_start(conn, mechanism_list, NULL, &raw_payload, &raw_payload_len, &mechanism_selected);
  if (is_sasl_failure(result)) {
    return Qfalse;
  }

  if (result != SASL_CONTINUE) {
    return Qfalse;
  }

  result = sasl_encode64(raw_payload, raw_payload_len, encoded_payload, sizeof(encoded_payload), &encoded_payload_len);
  if (is_sasl_failure(result)) {
    return Qfalse;
  }

  encoded_payload[encoded_payload_len] = 0;
  return rb_str_new(encoded_payload, encoded_payload_len);
}

static VALUE evaluate_challenge(VALUE self, VALUE rb_payload) {
  char base_payload[4096], payload[4096];
  const char *step_payload, *out;
  unsigned int step_payload_len, payload_len, base_payload_len, outlen;
  int result;
  sasl_conn_t *conn = mongo_sasl_context(self);

  StringValue(rb_payload);
  step_payload = RSTRING_PTR(rb_payload);
  step_payload_len = RSTRING_LEN(rb_payload);

  result = sasl_decode64(step_payload, step_payload_len, base_payload, sizeof(base_payload), &base_payload_len);
  if (is_sasl_failure(result)) {
    return Qfalse;
  }

  result = sasl_client_step(conn, base_payload, base_payload_len, NULL, &out, &outlen);
  if (is_sasl_failure(result)) {
  	return Qfalse;
  }

  result = sasl_encode64(out, outlen, payload, sizeof(payload), &payload_len);
  if (is_sasl_failure(result)) {
    return Qfalse;
  }

  return rb_str_new(payload, payload_len);
}

VALUE c_GSSAPI_authenticator;

void Init_csasl() {
  VALUE mongo, sasl;
  mongo = rb_const_get(rb_cObject, rb_intern("Mongo"));
  sasl = rb_const_get(mongo, rb_intern("Sasl"));
  c_GSSAPI_authenticator = rb_define_class_under(sasl, "GSSAPIAuthenticator", rb_cObject);
  rb_define_method(c_GSSAPI_authenticator, "initialize", a_init, 4);
  rb_define_method(c_GSSAPI_authenticator, "initialize_challenge", initialize_challenge, 0);
  rb_define_method(c_GSSAPI_authenticator, "evaluate_challenge", evaluate_challenge, 1);
  rb_define_method(rb_cObject, "valid?", valid, 0);
}
