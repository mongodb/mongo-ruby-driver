#include "ruby.h"
#include <assert.h>

#define INITIAL_BUFFER_SIZE 256

typedef struct {
    char* buffer;
    int size;
    int position;
} bson_buffer;

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

static VALUE method_serialize(VALUE self, VALUE doc) {
    char data[5] = {0x05, 0x00, 0x00, 0x00, 0x00};
    return rb_str_new(data, 5);
}

void Init_cbson() {
    VALUE CBson = rb_define_module("CBson");
    rb_define_module_function(CBson, "serialize", method_serialize, 1);
}
