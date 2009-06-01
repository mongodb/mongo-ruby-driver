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
#include "ruby/st.h"
#include "ruby/regex.h"
#include <assert.h>

#define INITIAL_BUFFER_SIZE 256

static VALUE Binary;
static VALUE Undefined;
static VALUE Time;
static VALUE ObjectID;
static VALUE DBRef;
static VALUE Code;
static VALUE RegexpOfHolding;
static VALUE OrderedHash;

// this sucks. but for some reason these moved around between 1.8 and 1.9
#ifdef ONIGURUMA_H
#define IGNORECASE ONIG_OPTION_IGNORECASE
#define MULTILINE ONIG_OPTION_MULTILINE
#define EXTENDED ONIG_OPTION_EXTEND
#else
#define IGNORECASE RE_OPTION_IGNORECASE
#define MULTILINE RE_OPTION_MULTILINE
#define EXTENDED RE_OPTION_EXTENDED
#endif

// this sucks too.
#ifndef RREGEXP_SRC_PTR
#define RREGEXP_SRC_PTR(r) RREGEXP(r)->str
#define RREGEXP_SRC_LEN(r) RREGEXP(r)->len
#endif

typedef struct {
    char* buffer;
    int size;
    int position;
} bson_buffer;

static char zero = 0;
static char one = 1;

static int cmp_char(const void* a, const void* b) {
    return *(char*)a - *(char*)b;
}

static void write_doc(bson_buffer* buffer, VALUE hash, VALUE no_dollar_sign);
static int write_element(VALUE key, VALUE value, VALUE extra);
static VALUE elements_to_hash(const char* buffer, int max);

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

static VALUE pack_extra(bson_buffer* buffer, VALUE no_dollar_sign) {
    return rb_ary_new3(2, INT2NUM((int)buffer), no_dollar_sign);
}

static void write_name_and_type(bson_buffer* buffer, VALUE name, char type) {
    buffer_write_bytes(buffer, &type, 1);
    buffer_write_bytes(buffer, RSTRING_PTR(name), RSTRING_LEN(name));
    buffer_write_bytes(buffer, &zero, 1);
}

static int write_element_allow_id(VALUE key, VALUE value, VALUE extra, int allow_id) {
    bson_buffer* buffer = (bson_buffer*)NUM2INT(rb_ary_entry(extra, 0));
    VALUE no_dollar_sign = rb_ary_entry(extra, 1);

    if (TYPE(key) == T_SYMBOL) {
        // TODO better way to do this... ?
        key = rb_str_new2(rb_id2name(SYM2ID(key)));
    }

    if (TYPE(key) != T_STRING) {
        rb_raise(rb_eTypeError, "keys must be strings or symbols");
    }

    if (!allow_id && strcmp("_id", RSTRING_PTR(key)) == 0) {
        return ST_CONTINUE;
    }

    if (no_dollar_sign == Qtrue && RSTRING_LEN(key) > 0 && RSTRING_PTR(key)[0] == '$') {
        rb_raise(rb_eRuntimeError, "key must not start with '$'");
    }
    int i;
    for (i = 0; i < RSTRING_LEN(key); i++) {
        if (RSTRING_PTR(key)[i] == '.') {
            rb_raise(rb_eRuntimeError, "key must not contain '.'");
        }
    }

    switch(TYPE(value)) {
    case T_BIGNUM:
        {
            if (rb_funcall(value, rb_intern(">"), 1, INT2NUM(2147483647)) == Qtrue ||
                rb_funcall(value, rb_intern("<"), 1, INT2NUM(-2147483648)) == Qtrue) {
                rb_raise(rb_eRangeError, "MongoDB can only handle 4-byte ints - try converting to a double before saving");
            }
            write_name_and_type(buffer, key, 0x10);
            VALUE as_f = rb_funcall(value, rb_intern("to_f"), 0);
            int int_value = NUM2LL(as_f);
            buffer_write_bytes(buffer, (char*)&int_value, 4);
            break;
        }
    case T_FIXNUM:
        {
            write_name_and_type(buffer, key, 0x10);
            int int_value = FIX2INT(value);
            buffer_write_bytes(buffer, (char*)&int_value, 4);
            break;
        }
    case T_TRUE:
        {
            write_name_and_type(buffer, key, 0x08);
            buffer_write_bytes(buffer, &one, 1);
            break;
        }
    case T_FALSE:
        {
            write_name_and_type(buffer, key, 0x08);
            buffer_write_bytes(buffer, &zero, 1);
            break;
        }
    case T_FLOAT:
        {
            write_name_and_type(buffer, key, 0x01);
            double d = NUM2DBL(value);
            buffer_write_bytes(buffer, (char*)&d, 8);
            break;
        }
    case T_NIL:
        {
            write_name_and_type(buffer, key, 0x0A);
            break;
        }
    case T_HASH:
        {
            write_name_and_type(buffer, key, 0x03);
            write_doc(buffer, value, no_dollar_sign);
            break;
        }
    case T_ARRAY:
        {
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
                write_element(key, values[i], pack_extra(buffer, no_dollar_sign));
                free(name);
            }

            // write null byte and fill in length
            buffer_write_bytes(buffer, &zero, 1);
            int obj_length = buffer->position - start_position;
            memcpy(buffer->buffer + length_location, &obj_length, 4);
            break;
        }
    case T_STRING:
        {
            if (strcmp(rb_class2name(RBASIC(value)->klass),
                       "XGen::Mongo::Driver::Code") == 0) {
                write_name_and_type(buffer, key, 0x0F);

                int start_position = buffer->position;
                int length_location = buffer_save_bytes(buffer, 4);

                int length = RSTRING_LEN(value) + 1;
                buffer_write_bytes(buffer, (char*)&length, 4);
                buffer_write_bytes(buffer, RSTRING_PTR(value), length - 1);
                buffer_write_bytes(buffer, &zero, 1);
                write_doc(buffer, rb_funcall(value, rb_intern("scope"), 0), Qfalse);

                int total_length = buffer->position - start_position;
                memcpy(buffer->buffer + length_location, &total_length, 4);

                break;
            } else {
                write_name_and_type(buffer, key, 0x02);
                int length = RSTRING_LEN(value) + 1;
                buffer_write_bytes(buffer, (char*)&length, 4);
                buffer_write_bytes(buffer, RSTRING_PTR(value), length - 1);
                buffer_write_bytes(buffer, &zero, 1);
                break;
            }
        }
    case T_SYMBOL:
        {
            write_name_and_type(buffer, key, 0x0E);
            const char* str_value = rb_id2name(SYM2ID(value));
            int length = strlen(str_value) + 1;
            buffer_write_bytes(buffer, (char*)&length, 4);
            buffer_write_bytes(buffer, str_value, length);
            break;
        }
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
                int length = RSTRING_LEN(string_data);
                if (subtype == 2) {
                    const int other_length = length + 4;
                    buffer_write_bytes(buffer, (const char*)&other_length, 4);
                    buffer_write_bytes(buffer, &subtype, 1);
                }
                buffer_write_bytes(buffer, (const char*)&length, 4);
                if (subtype != 2) {
                    buffer_write_bytes(buffer, &subtype, 1);
                }
                buffer_write_bytes(buffer, RSTRING_PTR(string_data), length);
                break;
            }
            if (strcmp(cls, "XGen::Mongo::Driver::ObjectID") == 0) {
                write_name_and_type(buffer, key, 0x07);
                VALUE as_array = rb_funcall(value, rb_intern("to_a"), 0);
                int i;
                for (i = 0; i < 12; i++) {
                    char byte = (char)FIX2INT(RARRAY_PTR(as_array)[i]);
                    buffer_write_bytes(buffer, &byte, 1);
                }
                break;
            }
            if (strcmp(cls, "XGen::Mongo::Driver::DBRef") == 0) {
                write_name_and_type(buffer, key, 0x03);

                int start_position = buffer->position;

                // save space for length
                int length_location = buffer_save_bytes(buffer, 4);

                VALUE ns = rb_funcall(value, rb_intern("namespace"), 0);
                write_element(rb_str_new2("$ref"), ns, pack_extra(buffer, Qfalse));
                VALUE oid = rb_funcall(value, rb_intern("object_id"), 0);
                write_element(rb_str_new2("$id"), oid, pack_extra(buffer, Qfalse));

                // write null byte and fill in length
                buffer_write_bytes(buffer, &zero, 1);
                int obj_length = buffer->position - start_position;
                memcpy(buffer->buffer + length_location, &obj_length, 4);
                break;
            }
            if (strcmp(cls, "XGen::Mongo::Driver::Undefined") == 0) {
                write_name_and_type(buffer, key, 0x06);
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
    case T_REGEXP:
        {
            write_name_and_type(buffer, key, 0x0B);

            int length = RREGEXP_SRC_LEN(value);
            char* pattern = (char*)RREGEXP_SRC_PTR(value);
            buffer_write_bytes(buffer, pattern, length);
            buffer_write_bytes(buffer, &zero, 1);

            long flags = RREGEXP(value)->ptr->options;
            if (flags & IGNORECASE) {
                char ignorecase = 'i';
                buffer_write_bytes(buffer, &ignorecase, 1);
            }
            if (flags & MULTILINE) {
                char multiline = 'm';
                buffer_write_bytes(buffer, &multiline, 1);
            }
            if (flags & EXTENDED) {
                char extended = 'x';
                buffer_write_bytes(buffer, &extended, 1);
            }

            VALUE has_extra = rb_funcall(value, rb_intern("respond_to?"), 1, rb_str_new2("extra_options_str"));
            if (TYPE(has_extra) == T_TRUE) {
                VALUE extra = rb_funcall(value, rb_intern("extra_options_str"), 0);
                int old_position = buffer->position;
                buffer_write_bytes(buffer, RSTRING_PTR(extra), RSTRING_LEN(extra));
                qsort(buffer->buffer + old_position, RSTRING_LEN(extra), sizeof(char), cmp_char);
            }
            buffer_write_bytes(buffer, &zero, 1);

            break;
        }
    default:
        {
            rb_raise(rb_eTypeError, "no c encoder for this type yet (%d)", TYPE(value));
            break;
        }
    }
    return ST_CONTINUE;
}

static int write_element(VALUE key, VALUE value, VALUE extra) {
    return write_element_allow_id(key, value, extra, 0);
}

static void write_doc(bson_buffer* buffer, VALUE hash, VALUE no_dollar_sign) {
    int start_position = buffer->position;
    int length_location = buffer_save_bytes(buffer, 4);

    VALUE key = rb_str_new2("_id");
    if (rb_funcall(hash, rb_intern("has_key?"), 1, key) == Qtrue) {
        VALUE id = rb_hash_aref(hash, key);
        write_element_allow_id(key, id, pack_extra(buffer, no_dollar_sign), 1);
    }
    key = ID2SYM(rb_intern("_id"));
    if (rb_funcall(hash, rb_intern("has_key?"), 1, key) == Qtrue) {
        VALUE id = rb_hash_aref(hash, key);
        write_element_allow_id(key, id, pack_extra(buffer, no_dollar_sign), 1);
    }

    // we have to check for an OrderedHash and handle that specially
    if (strcmp(rb_class2name(RBASIC(hash)->klass), "OrderedHash") == 0) {
        VALUE keys = rb_funcall(hash, rb_intern("keys"), 0);
        int i;
        for(i = 0; i < RARRAY_LEN(keys); i++) {
            VALUE key = RARRAY_PTR(keys)[i];
            VALUE value = rb_hash_aref(hash, key);

            write_element(key, value, pack_extra(buffer, no_dollar_sign));
        }
    } else {
        rb_hash_foreach(hash, write_element, pack_extra(buffer, no_dollar_sign));
    }

    // write null byte and fill in length
    buffer_write_bytes(buffer, &zero, 1);
    int length = buffer->position - start_position;
    memcpy(buffer->buffer + length_location, &length, 4);
}

static VALUE method_serialize(VALUE self, VALUE doc, VALUE no_dollar_sign) {
    bson_buffer* buffer = buffer_new();
    assert(buffer);

    write_doc(buffer, doc, no_dollar_sign);

    VALUE result = rb_str_new(buffer->buffer, buffer->position);
    buffer_free(buffer);
    return result;
}

static VALUE get_value(const char* buffer, int* position, int type) {
    VALUE value;
    switch (type) {
    case 1:
        {
            double d;
            memcpy(&d, buffer + *position, 8);
            value = rb_float_new(d);
            *position += 8;
            break;
        }
    case 2:
    case 13:
        {
            *position += 4;
            int value_length = strlen(buffer + *position);
            value = rb_str_new(buffer+ *position, value_length);
            *position += value_length + 1;
            break;
        }
    case 3:
        {
            int size;
            memcpy(&size, buffer + *position, 4);
            if (strcmp(buffer + *position + 5, "$ref") == 0) { // DBRef
                int offset = *position + 14;
                VALUE argv[2];
                int collection_length = strlen(buffer + offset);
                argv[0] = rb_str_new(buffer + offset, collection_length);
                offset += collection_length + 1;
                char id_type = buffer[offset];
                offset += 5;
                argv[1] = get_value(buffer, &offset, (int)id_type);
                value = rb_class_new_instance(2, argv, DBRef);
            } else {
                value = elements_to_hash(buffer + *position + 4, size - 5);
            }
            *position += size;
            break;
        }
    case 4:
        {
            int size;
            memcpy(&size, buffer + *position, 4);
            int end = *position + size - 1;
            *position += 4;

            value = rb_ary_new();
            while (*position < end) {
                int type = (int)buffer[(*position)++];
                int key_size = strlen(buffer + *position);
                *position += key_size + 1; // just skip the key, they're in order.
                VALUE to_append = get_value(buffer, position, type);
                rb_ary_push(value, to_append);
            }
            (*position)++;
            break;
        }
    case 5:
        {
            int length;
            memcpy(&length, buffer + *position, 4);
            int subtype = (unsigned char)buffer[*position + 4];
            VALUE data;
            if (subtype == 2) {
                data = rb_str_new(buffer + *position + 9, length - 4);
            } else {
                data = rb_str_new(buffer + *position + 5, length);
            }
            VALUE st = INT2FIX(subtype);
            VALUE argv[2] = {data, st};
            value = rb_class_new_instance(2, argv, Binary);
            *position += length + 5;
            break;
        }
    case 6:
        {
            value = rb_class_new_instance(0, NULL, Undefined);
            break;
        }
    case 7:
        {
            VALUE str = rb_str_new(buffer + *position, 12);
            VALUE oid = rb_funcall(str, rb_intern("unpack"), 1, rb_str_new2("C*"));
            value = rb_class_new_instance(1, &oid, ObjectID);
            *position += 12;
            break;
        }
    case 8:
        {
            value = buffer[(*position)++] ? Qtrue : Qfalse;
            break;
        }
    case 9:
        {
            long long millis;
            memcpy(&millis, buffer + *position, 8);
            VALUE seconds = INT2NUM(millis / 1000);
            VALUE microseconds = INT2NUM((millis % 1000) * 1000);

            value = rb_funcall(Time, rb_intern("at"), 2, seconds, microseconds);
            *position += 8;
            break;
        }
    case 10:
        {
            value = Qnil;
            break;
        }
    case 11:
        {
            int pattern_length = strlen(buffer + *position);
            VALUE pattern = rb_str_new(buffer + *position, pattern_length);
            *position += pattern_length + 1;

            int flags_length = strlen(buffer + *position);
            int i = 0;

            int flags = 0;
            char extra[10];
            extra[0] = 0;
            for (i = 0; i < flags_length; i++) {
                char flag = buffer[*position + i];
                if (flag == 'i') {
                    flags |= IGNORECASE;
                }
                else if (flag == 'm') {
                    flags |= MULTILINE;
                }
                else if (flag == 'x') {
                    flags |= EXTENDED;
                }
                else if (strlen(extra) < 9) {
                    strncat(extra, &flag, 1);
                }
            }
            VALUE argv[3] = {
                pattern,
                INT2FIX(flags),
                rb_str_new2(extra)
            };
            value = rb_class_new_instance(3, argv, RegexpOfHolding);
            *position += flags_length + 1;
            break;
        }
    case 12:
        {
            *position += 4;
            int collection_length = strlen(buffer + *position);
            VALUE collection = rb_str_new(buffer + *position, collection_length);
            *position += collection_length + 1;

            VALUE str = rb_str_new(buffer + *position, 12);
            VALUE oid = rb_funcall(str, rb_intern("unpack"), 1, rb_str_new2("C*"));
            VALUE id = rb_class_new_instance(1, &oid, ObjectID);
            *position += 12;

            VALUE argv[2] = {collection, id};
            value = rb_class_new_instance(2, argv, DBRef);
            break;
        }
    case 14:
        {
            int value_length;
            memcpy(&value_length, buffer + *position, 4);
            value = ID2SYM(rb_intern(buffer + *position + 4));
            *position += value_length + 4;
            break;
        }
    case 15:
        {
            *position += 8;
            int code_length = strlen(buffer + *position);
            VALUE code = rb_str_new(buffer + *position, code_length);
            *position += code_length + 1;

            int scope_size;
            memcpy(&scope_size, buffer + *position, 4);
            VALUE scope = elements_to_hash(buffer + *position + 4, scope_size - 5);
            *position += scope_size;

            VALUE argv[2] = {code, scope};
            value = rb_class_new_instance(2, argv, Code);
            break;
        }
    case 16:
        {
            int i;
            memcpy(&i, buffer + *position, 4);
            value = LL2NUM(i);
            *position += 4;
            break;
        }
    case 17:
        {
            int i;
            int j;
            memcpy(&i, buffer + *position, 4);
            memcpy(&j, buffer + *position + 4, 4);
            value = rb_ary_new3(2, LL2NUM(i), LL2NUM(j));
            *position += 8;
            break;
        }
    default:
        {
            rb_raise(rb_eTypeError, "no c decoder for this type yet (%d)", type);
            break;
        }
    }
    return value;
}

static VALUE elements_to_hash(const char* buffer, int max) {
    VALUE hash = rb_class_new_instance(0, NULL, OrderedHash);
    int position = 0;
    while (position < max) {
        int type = (int)buffer[position++];
        int name_length = strlen(buffer + position);
        VALUE name = rb_str_new(buffer + position, name_length);
        position += name_length + 1;
        VALUE value = get_value(buffer, &position, type);
        rb_funcall(hash, rb_intern("[]="), 2, name, value);
    }
    return hash;
}

static VALUE method_deserialize(VALUE self, VALUE bson) {
    const char* buffer = RSTRING_PTR(bson);
    int remaining = RSTRING_LEN(bson);

    // NOTE we just swallow the size and end byte here
    buffer += 4;
    remaining -= 5;

    return elements_to_hash(buffer, remaining);
}

void Init_cbson() {
    Time = rb_const_get(rb_cObject, rb_intern("Time"));

    VALUE driver = rb_const_get(rb_const_get(rb_const_get(rb_cObject,
                                                          rb_intern("XGen")),
                                             rb_intern("Mongo")),
                                rb_intern("Driver"));
    rb_require("mongo/types/binary");
    Binary = rb_const_get(driver, rb_intern("Binary"));
    rb_require("mongo/types/undefined");
    Undefined = rb_const_get(driver, rb_intern("Undefined"));
    rb_require("mongo/types/objectid");
    ObjectID = rb_const_get(driver, rb_intern("ObjectID"));
    rb_require("mongo/types/dbref");
    DBRef = rb_const_get(driver, rb_intern("DBRef"));
    rb_require("mongo/types/code");
    Code = rb_const_get(driver, rb_intern("Code"));
    rb_require("mongo/types/regexp_of_holding");
    RegexpOfHolding = rb_const_get(driver, rb_intern("RegexpOfHolding"));
    rb_require("mongo/util/ordered_hash");
    OrderedHash = rb_const_get(rb_cObject, rb_intern("OrderedHash"));

    VALUE CBson = rb_define_module("CBson");
    rb_define_module_function(CBson, "serialize", method_serialize, 2);
    rb_define_module_function(CBson, "deserialize", method_deserialize, 1);
}
