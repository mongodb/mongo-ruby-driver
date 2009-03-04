#include "ruby.h"
#include "st.h"
#include <assert.h>

#define INITIAL_BUFFER_SIZE 256

typedef struct {
    char* buffer;
    int size;
    int position;
} bson_buffer;

static char zero = 0;

static bson_buffer* buffer_new(void) {
    bson_buffer* buffer;
    buffer = (bson_buffer*)malloc(sizeof(bson_buffer));
    assert(buffer);

    buffer->size = INITIAL_BUFFER_SIZE;
    buffer->position = 0;
    buffer->buffer = (char*)malloc(INITIAL_BUFFER_SIZE);
    assert(buffer->buffer);

    return buffer;
}

static void buffer_free(bson_buffer* buffer) {
    assert(buffer);
    assert(buffer->buffer);

    free(buffer->buffer);
    free(buffer);
}

static void buffer_resize(bson_buffer* buffer, int min_length) {
    int size = buffer->size;
    if (size >= min_length) {
        return;
    }
    while (size < min_length) {
        size *= 2;
    }
    buffer->buffer = (char*)realloc(buffer->buffer, size);
    assert(buffer->buffer);
    buffer->size = size;
}

static void buffer_assure_space(bson_buffer* buffer, int size) {
    if (buffer->position + size <= buffer->size) {
        return;
    }
    buffer_resize(buffer, buffer->position + size);
}

/* returns offset for writing */
static int buffer_save_bytes(bson_buffer* buffer, int size) {
    buffer_assure_space(buffer, size);
    int position = buffer->position;
    buffer->position += size;
    return position;
}

static void buffer_write_bytes(bson_buffer* buffer, const char* bytes, int size) {
    buffer_assure_space(buffer, size);

    memcpy(buffer->buffer + buffer->position, bytes, size);
    buffer->position += size;
}

static void write_name_and_type(bson_buffer* buffer, VALUE name, char type) {
    buffer_write_bytes(buffer, &type, 1);
    buffer_write_bytes(buffer, RSTRING(name)->ptr, RSTRING(name)->len);
    buffer_write_bytes(buffer, &zero, 1);
}

static int write_element(VALUE key, VALUE value, VALUE extra) {
    bson_buffer* buffer = (bson_buffer*)extra;

    switch(TYPE(value)) {
    case T_STRING:
        write_name_and_type(buffer, key, 0x02);
        int length = RSTRING(value)->len + 1;
        buffer_write_bytes(buffer, (char*)&length, 4);
        buffer_write_bytes(buffer, RSTRING(value)->ptr, length - 1);
        buffer_write_bytes(buffer, &zero, 1);
        break;
    default:
        rb_raise(rb_eTypeError, "no c encoder for this type yet");
        break;
    }
    return ST_CONTINUE;
}

static void write_doc(bson_buffer* buffer, VALUE hash) {
    int start_position = buffer->position;
    int length_location = buffer_save_bytes(buffer, 4);

    rb_hash_foreach(hash, write_element, (VALUE)buffer);

    // write null byte and fill in length
    buffer_write_bytes(buffer, &zero, 1);
    int length = buffer->position - start_position;
    memcpy(buffer->buffer + length_location, &length, 4);
}

static VALUE method_serialize(VALUE self, VALUE doc) {
    bson_buffer* buffer = buffer_new();
    assert(buffer);

    write_doc(buffer, doc);

    VALUE result = rb_str_new(buffer->buffer, buffer->position);
    buffer_free(buffer);
    return result;
}

void Init_cbson() {
    VALUE CBson = rb_define_module("CBson");
    rb_define_module_function(CBson, "serialize", method_serialize, 1);
}
