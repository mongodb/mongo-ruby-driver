/*
 * Copyright 2009 10gen, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This file contains C implementations of some of the functions needed by the
 * bson module. If possible, these implementations should be used to speed up
 * BSON encoding and decoding.
 */

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
static char one = 1;

static void write_doc(bson_buffer* buffer, VALUE hash);

static bson_buffer* buffer_new(void) {
    bson_buffer* buffer;
    buffer = ALLOC(bson_buffer);
    assert(buffer);

    buffer->size = INITIAL_BUFFER_SIZE;
    buffer->position = 0;
    buffer->buffer = ALLOC_N(char, INITIAL_BUFFER_SIZE);
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
    buffer->buffer = REALLOC_N(buffer->buffer, char, size);
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

    if (TYPE(key) == T_SYMBOL) {
        // TODO better way to do this... ?
        key = rb_str_new2(rb_id2name(SYM2ID(key)));
    }

    if (TYPE(key) != T_STRING) {
        rb_raise(rb_eTypeError, "keys must be strings or symbols");
    }

    switch(TYPE(value)) {
    case T_FIXNUM:
        write_name_and_type(buffer, key, 0x10);
        int int_value = FIX2INT(value);
        buffer_write_bytes(buffer, (char*)&int_value, 4);
        break;
    case T_TRUE:
        write_name_and_type(buffer, key, 0x08);
        buffer_write_bytes(buffer, &one, 1);
        break;
    case T_FALSE:
        write_name_and_type(buffer, key, 0x08);
        buffer_write_bytes(buffer, &zero, 1);
        break;
    case T_FLOAT:
        write_name_and_type(buffer, key, 0x01);
        double d = NUM2DBL(value);
        buffer_write_bytes(buffer, (char*)&d, 8);
        break;
    case T_NIL:
        write_name_and_type(buffer, key, 0x0A);
        break;
    case T_HASH:
        write_name_and_type(buffer, key, 0x03);
        write_doc(buffer, value);
        break;
    case T_ARRAY:
        write_name_and_type(buffer, key, 0x04);
        int start_position = buffer->position;

        // save space for length
        int length_location = buffer_save_bytes(buffer, 4);

        int items = RARRAY_LEN(value);
        VALUE* values = RARRAY_PTR(value);
        int i;
        for(i = 0; i < items; i++) {
            char* name;
            asprintf(&name, "%d", i);
            VALUE key = rb_str_new2(name);
            write_element(key, values[i], (VALUE)buffer);
            free(name);
        }

        // write null byte and fill in length
        buffer_write_bytes(buffer, &zero, 1);
        int obj_length = buffer->position - start_position;
        memcpy(buffer->buffer + length_location, &obj_length, 4);
        break;
    case T_STRING:
        write_name_and_type(buffer, key, 0x02);
        int length = RSTRING(value)->len + 1;
        buffer_write_bytes(buffer, (char*)&length, 4);
        buffer_write_bytes(buffer, RSTRING(value)->ptr, length - 1);
        buffer_write_bytes(buffer, &zero, 1);
        break;
    case T_SYMBOL:
        write_name_and_type(buffer, key, 0x0E);
        const char* str_value = rb_id2name(SYM2ID(value));
        int str_length = strlen(str_value) + 1;
        buffer_write_bytes(buffer, (char*)&str_length, 4);
        buffer_write_bytes(buffer, str_value, str_length);
        break;
    case T_OBJECT:
        {
            // TODO there has to be a better way to do these checks...
            const char* cls = rb_class2name(RBASIC(value)->klass);
            if (strcmp(cls, "XGen::Mongo::Driver::Binary") == 0 ||
                strcmp(cls, "ByteBuffer") == 0) {
                write_name_and_type(buffer, key, 0x05);
                const char subtype = strcmp(cls, "ByteBuffer") ?
                    (const char)FIX2INT(rb_funcall(value, rb_intern("subtype"), 0)) : 2;
                VALUE string_data = rb_funcall(value, rb_intern("to_s"), 0);
                int length = RSTRING(string_data)->len;
                if (subtype == 2) {
                    const int other_length = length + 4;
                    buffer_write_bytes(buffer, (const char*)&other_length, 4);
                    buffer_write_bytes(buffer, &subtype, 1);
                }
                buffer_write_bytes(buffer, (const char*)&length, 4);
                if (subtype != 2) {
                    buffer_write_bytes(buffer, &subtype, 1);
                }
                buffer_write_bytes(buffer, RSTRING(string_data)->ptr, length);
                break;
            }
        }
    case T_DATA:
        {
            // TODO again, is this really the only way to do this?
            const char* cls = rb_class2name(RBASIC(value)->klass);
            if (strcmp(cls, "Time") == 0) {
                write_name_and_type(buffer, key, 0x09);
                double t = NUM2DBL(rb_funcall(value, rb_intern("to_f"), 0));
                long long time_since_epoch = (long long)(t * 1000);
                buffer_write_bytes(buffer, (const char*)&time_since_epoch, 8);
                break;
            }
        }
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
