#include <ruby.h>

#include <sasl/sasl.h>
#include <sasl/saslutil.h>



static VALUE a_init(VALUE self, VALUE username, VALUE hostname, VALUE servicename, VALUE canonicalizehostname)
{
  rb_iv_set(self, "@username", username);
  rb_iv_set(self, "@hostname", hostname);
  rb_iv_set(self, "@servicename", servicename);
  //rb_iv_set(self, "@canonicalizehostname", canonicalizehostname);

  return self;
}

// auxiliary functions
int is_sasl_failure(int result, char **error_message)
{
	if (result < 0) {
		*error_message = malloc(256);
		snprintf(*error_message, 256, "Authentication error: %s", sasl_errstring(result, NULL, NULL));
		return 1;
	}

	return 0;
}

char *rb_mongo_saslstart(sasl_conn_t *conn, char **out_payload, int *out_payload_len, int32_t *conversation_id, char **error_message)
{
	const char *raw_payload;
	char encoded_payload[4096];
	unsigned int raw_payload_len, encoded_payload_len;
	int result;
	char *mechanism_list = "GSSAPI";
	const char *mechanism_selected;
	sasl_interact_t *client_interact=NULL;

	result = sasl_client_start(conn, mechanism_list, &client_interact, &raw_payload, &raw_payload_len, &mechanism_selected);
	if (is_sasl_failure(result, error_message)) {
		return NULL;
	}

	if (result != SASL_CONTINUE) {
		*error_message = strdup("Could not negotiate SASL mechanism");
		return NULL;
	}

	mechanism_selected = "GSSAPI";


	result = sasl_encode64(raw_payload, raw_payload_len, encoded_payload, sizeof(encoded_payload), &encoded_payload_len);
	if (is_sasl_failure(result, error_message)) {
		return NULL;
	}

	return encoded_payload;
}

static VALUE initialize_challenge(VALUE self) {
    int result;
	char *initpayload;
	int initpayload_len;
	char **error_message;
	sasl_conn_t *conn;
	int32_t conversation_id;
	//sasl_callback_t client_interact=NULL;
	char *payload;

	const char *servicename = RSTRING_PTR(rb_iv_get(self, "@servicename"));
	const char *hostname = RSTRING_PTR(rb_iv_get(self, "@hostname"));

	result = sasl_client_new(servicename, hostname, NULL, NULL, NULL, 0, &conn);

	if (result != SASL_OK) {
    	sasl_dispose(&conn);
    	*error_message = strdup("Could not initialize a client exchange (SASL) to MongoDB");
        return 0;
    }

    payload = rb_mongo_saslstart(conn, &initpayload, &initpayload_len, &conversation_id, error_message);
    if (!conn) {
    	return 0;
    }

    rb_iv_set(self, "@context", conn);

    return rb_str_new2(payload);
}


static VALUE evaluate_challenge(VALUE self, VALUE rb_payload) {

    sasl_interact_t *client_interact=NULL;

	char step_payload[4096], base_payload[4096], payload[4096];
	unsigned int step_payload_len, base_payload_len, payload_len;
	const char *out;
	unsigned int outlen;
	unsigned char done = 0;
	char **error_message;
	int result;

	step_payload = RSTRING_PTR(rb_payload);
	step_payload_len = RSTRING_LEN(rb_payload);

	step_payload_len--; /* Remove the \0 from the string */
	result = sasl_decode64(RSTRING_PTR(rb_payload), step_payload_len, base_payload, sizeof(base_payload), &base_payload_len);
	if (is_sasl_failure(result, error_message)) {
		return 0;
	}

    sasl_conn_t *conn = rb_iv_get(self, "@context");

	result = sasl_client_step(conn, (const char *)base_payload, base_payload_len, &client_interact, &out, &outlen);
	if (is_sasl_failure(result, error_message)) {
		return 0;
	}

	result = sasl_encode64(out, outlen, payload, sizeof(base_payload), &payload_len);
	if (is_sasl_failure(result, error_message)) {
		return 0;
	}

	return rb_str_new2(payload);
}


// define the class
VALUE c_GSSAPI_authenticator;

void Init_GSSAPIAuthenticator() {
  c_GSSAPI_authenticator = rb_define_class("Mongo::SASL::GSSAPIAuthenticator", rb_cObject);
  rb_define_method(c_GSSAPI_authenticator, "initialize", a_init, 4);
  rb_define_method(c_GSSAPI_authenticator, "initialize_challenge", initialize_challenge, 0);
  rb_define_method(c_GSSAPI_authenticator, "evaluate_challenge", evaluate_challenge, 0);
}