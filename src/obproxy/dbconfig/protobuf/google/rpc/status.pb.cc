// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: google/rpc/status.proto

#include "google/rpc/status.pb.h"

#include <algorithm>

#include <google/protobuf/stubs/common.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/extension_set.h>
#include <google/protobuf/wire_format_lite_inl.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/generated_message_reflection.h>
#include <google/protobuf/reflection_ops.h>
#include <google/protobuf/wire_format.h>
// @@protoc_insertion_point(includes)
#include <google/protobuf/port_def.inc>

extern PROTOBUF_INTERNAL_EXPORT_google_2fprotobuf_2fany_2eproto ::google::protobuf::internal::SCCInfo<0> scc_info_Any_google_2fprotobuf_2fany_2eproto;
namespace google {
namespace rpc {
class StatusDefaultTypeInternal {
 public:
  ::google::protobuf::internal::ExplicitlyConstructed<Status> _instance;
} _Status_default_instance_;
}  // namespace rpc
}  // namespace google
static void InitDefaultsStatus_google_2frpc_2fstatus_2eproto() {
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  {
    void* ptr = &::google::rpc::_Status_default_instance_;
    new (ptr) ::google::rpc::Status();
    ::google::protobuf::internal::OnShutdownDestroyMessage(ptr);
  }
  ::google::rpc::Status::InitAsDefaultInstance();
}

::google::protobuf::internal::SCCInfo<1> scc_info_Status_google_2frpc_2fstatus_2eproto =
    {{ATOMIC_VAR_INIT(::google::protobuf::internal::SCCInfoBase::kUninitialized), 1, InitDefaultsStatus_google_2frpc_2fstatus_2eproto}, {
      &scc_info_Any_google_2fprotobuf_2fany_2eproto.base,}};

void InitDefaults_google_2frpc_2fstatus_2eproto() {
  ::google::protobuf::internal::InitSCC(&scc_info_Status_google_2frpc_2fstatus_2eproto.base);
}

::google::protobuf::Metadata file_level_metadata_google_2frpc_2fstatus_2eproto[1];
constexpr ::google::protobuf::EnumDescriptor const** file_level_enum_descriptors_google_2frpc_2fstatus_2eproto = nullptr;
constexpr ::google::protobuf::ServiceDescriptor const** file_level_service_descriptors_google_2frpc_2fstatus_2eproto = nullptr;

const ::google::protobuf::uint32 TableStruct_google_2frpc_2fstatus_2eproto::offsets[] PROTOBUF_SECTION_VARIABLE(protodesc_cold) = {
  ~0u,  // no _has_bits_
  PROTOBUF_FIELD_OFFSET(::google::rpc::Status, _internal_metadata_),
  ~0u,  // no _extensions_
  ~0u,  // no _oneof_case_
  ~0u,  // no _weak_field_map_
  PROTOBUF_FIELD_OFFSET(::google::rpc::Status, code_),
  PROTOBUF_FIELD_OFFSET(::google::rpc::Status, message_),
  PROTOBUF_FIELD_OFFSET(::google::rpc::Status, details_),
};
static const ::google::protobuf::internal::MigrationSchema schemas[] PROTOBUF_SECTION_VARIABLE(protodesc_cold) = {
  { 0, -1, sizeof(::google::rpc::Status)},
};

static ::google::protobuf::Message const * const file_default_instances[] = {
  reinterpret_cast<const ::google::protobuf::Message*>(&::google::rpc::_Status_default_instance_),
};

::google::protobuf::internal::AssignDescriptorsTable assign_descriptors_table_google_2frpc_2fstatus_2eproto = {
  {}, AddDescriptors_google_2frpc_2fstatus_2eproto, "google/rpc/status.proto", schemas,
  file_default_instances, TableStruct_google_2frpc_2fstatus_2eproto::offsets,
  file_level_metadata_google_2frpc_2fstatus_2eproto, 1, file_level_enum_descriptors_google_2frpc_2fstatus_2eproto, file_level_service_descriptors_google_2frpc_2fstatus_2eproto,
};

const char descriptor_table_protodef_google_2frpc_2fstatus_2eproto[] =
  "\n\027google/rpc/status.proto\022\ngoogle.rpc\032\031g"
  "oogle/protobuf/any.proto\"N\n\006Status\022\014\n\004co"
  "de\030\001 \001(\005\022\017\n\007message\030\002 \001(\t\022%\n\007details\030\003 \003"
  "(\0132\024.google.protobuf.AnyB^\n\016com.google.r"
  "pcB\013StatusProtoP\001Z7google.golang.org/gen"
  "proto/googleapis/rpc/status;status\242\002\003RPC"
  "b\006proto3"
  ;
::google::protobuf::internal::DescriptorTable descriptor_table_google_2frpc_2fstatus_2eproto = {
  false, InitDefaults_google_2frpc_2fstatus_2eproto, 
  descriptor_table_protodef_google_2frpc_2fstatus_2eproto,
  "google/rpc/status.proto", &assign_descriptors_table_google_2frpc_2fstatus_2eproto, 248,
};

void AddDescriptors_google_2frpc_2fstatus_2eproto() {
  static constexpr ::google::protobuf::internal::InitFunc deps[1] =
  {
    ::AddDescriptors_google_2fprotobuf_2fany_2eproto,
  };
 ::google::protobuf::internal::AddDescriptors(&descriptor_table_google_2frpc_2fstatus_2eproto, deps, 1);
}

// Force running AddDescriptors() at dynamic initialization time.
static bool dynamic_init_dummy_google_2frpc_2fstatus_2eproto = []() { AddDescriptors_google_2frpc_2fstatus_2eproto(); return true; }();
namespace google {
namespace rpc {

// ===================================================================

void Status::InitAsDefaultInstance() {
}
class Status::HasBitSetters {
 public:
};

void Status::clear_details() {
  details_.Clear();
}
#if !defined(_MSC_VER) || _MSC_VER >= 1900
const int Status::kCodeFieldNumber;
const int Status::kMessageFieldNumber;
const int Status::kDetailsFieldNumber;
#endif  // !defined(_MSC_VER) || _MSC_VER >= 1900

Status::Status()
  : ::google::protobuf::Message(), _internal_metadata_(nullptr) {
  SharedCtor();
  // @@protoc_insertion_point(constructor:google.rpc.Status)
}
Status::Status(const Status& from)
  : ::google::protobuf::Message(),
      _internal_metadata_(nullptr),
      details_(from.details_) {
  _internal_metadata_.MergeFrom(from._internal_metadata_);
  message_.UnsafeSetDefault(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
  if (from.message().size() > 0) {
    message_.AssignWithDefault(&::google::protobuf::internal::GetEmptyStringAlreadyInited(), from.message_);
  }
  code_ = from.code_;
  // @@protoc_insertion_point(copy_constructor:google.rpc.Status)
}

void Status::SharedCtor() {
  ::google::protobuf::internal::InitSCC(
      &scc_info_Status_google_2frpc_2fstatus_2eproto.base);
  message_.UnsafeSetDefault(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
  code_ = 0;
}

Status::~Status() {
  // @@protoc_insertion_point(destructor:google.rpc.Status)
  SharedDtor();
}

void Status::SharedDtor() {
  message_.DestroyNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
}

void Status::SetCachedSize(int size) const {
  _cached_size_.Set(size);
}
const Status& Status::default_instance() {
  ::google::protobuf::internal::InitSCC(&::scc_info_Status_google_2frpc_2fstatus_2eproto.base);
  return *internal_default_instance();
}


void Status::Clear() {
// @@protoc_insertion_point(message_clear_start:google.rpc.Status)
  ::google::protobuf::uint32 cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  details_.Clear();
  message_.ClearToEmptyNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
  code_ = 0;
  _internal_metadata_.Clear();
}

#if GOOGLE_PROTOBUF_ENABLE_EXPERIMENTAL_PARSER
const char* Status::_InternalParse(const char* begin, const char* end, void* object,
                  ::google::protobuf::internal::ParseContext* ctx) {
  auto msg = static_cast<Status*>(object);
  ::google::protobuf::int32 size; (void)size;
  int depth; (void)depth;
  ::google::protobuf::uint32 tag;
  ::google::protobuf::internal::ParseFunc parser_till_end; (void)parser_till_end;
  auto ptr = begin;
  while (ptr < end) {
    ptr = ::google::protobuf::io::Parse32(ptr, &tag);
    GOOGLE_PROTOBUF_PARSER_ASSERT(ptr);
    switch (tag >> 3) {
      // int32 code = 1;
      case 1: {
        if (static_cast<::google::protobuf::uint8>(tag) != 8) goto handle_unusual;
        msg->set_code(::google::protobuf::internal::ReadVarint(&ptr));
        GOOGLE_PROTOBUF_PARSER_ASSERT(ptr);
        break;
      }
      // string message = 2;
      case 2: {
        if (static_cast<::google::protobuf::uint8>(tag) != 18) goto handle_unusual;
        ptr = ::google::protobuf::io::ReadSize(ptr, &size);
        GOOGLE_PROTOBUF_PARSER_ASSERT(ptr);
        ctx->extra_parse_data().SetFieldName("google.rpc.Status.message");
        object = msg->mutable_message();
        if (size > end - ptr + ::google::protobuf::internal::ParseContext::kSlopBytes) {
          parser_till_end = ::google::protobuf::internal::GreedyStringParserUTF8;
          goto string_till_end;
        }
        GOOGLE_PROTOBUF_PARSER_ASSERT(::google::protobuf::internal::StringCheckUTF8(ptr, size, ctx));
        ::google::protobuf::internal::InlineGreedyStringParser(object, ptr, size, ctx);
        ptr += size;
        break;
      }
      // repeated .google.protobuf.Any details = 3;
      case 3: {
        if (static_cast<::google::protobuf::uint8>(tag) != 26) goto handle_unusual;
        do {
          ptr = ::google::protobuf::io::ReadSize(ptr, &size);
          GOOGLE_PROTOBUF_PARSER_ASSERT(ptr);
          parser_till_end = ::google::protobuf::Any::_InternalParse;
          object = msg->add_details();
          if (size > end - ptr) goto len_delim_till_end;
          ptr += size;
          GOOGLE_PROTOBUF_PARSER_ASSERT(ctx->ParseExactRange(
              {parser_till_end, object}, ptr - size, ptr));
          if (ptr >= end) break;
        } while ((::google::protobuf::io::UnalignedLoad<::google::protobuf::uint64>(ptr) & 255) == 26 && (ptr += 1));
        break;
      }
      default: {
      handle_unusual:
        if ((tag & 7) == 4 || tag == 0) {
          ctx->EndGroup(tag);
          return ptr;
        }
        auto res = UnknownFieldParse(tag, {_InternalParse, msg},
          ptr, end, msg->_internal_metadata_.mutable_unknown_fields(), ctx);
        ptr = res.first;
        GOOGLE_PROTOBUF_PARSER_ASSERT(ptr != nullptr);
        if (res.second) return ptr;
      }
    }  // switch
  }  // while
  return ptr;
string_till_end:
  static_cast<::std::string*>(object)->clear();
  static_cast<::std::string*>(object)->reserve(size);
  goto len_delim_till_end;
len_delim_till_end:
  return ctx->StoreAndTailCall(ptr, end, {_InternalParse, msg},
                               {parser_till_end, object}, size);
}
#else  // GOOGLE_PROTOBUF_ENABLE_EXPERIMENTAL_PARSER
bool Status::MergePartialFromCodedStream(
    ::google::protobuf::io::CodedInputStream* input) {
#define DO_(EXPRESSION) if (!PROTOBUF_PREDICT_TRUE(EXPRESSION)) goto failure
  ::google::protobuf::uint32 tag;
  // @@protoc_insertion_point(parse_start:google.rpc.Status)
  for (;;) {
    ::std::pair<::google::protobuf::uint32, bool> p = input->ReadTagWithCutoffNoLastTag(127u);
    tag = p.first;
    if (!p.second) goto handle_unusual;
    switch (::google::protobuf::internal::WireFormatLite::GetTagFieldNumber(tag)) {
      // int32 code = 1;
      case 1: {
        if (static_cast< ::google::protobuf::uint8>(tag) == (8 & 0xFF)) {

          DO_((::google::protobuf::internal::WireFormatLite::ReadPrimitive<
                   ::google::protobuf::int32, ::google::protobuf::internal::WireFormatLite::TYPE_INT32>(
                 input, &code_)));
        } else {
          goto handle_unusual;
        }
        break;
      }

      // string message = 2;
      case 2: {
        if (static_cast< ::google::protobuf::uint8>(tag) == (18 & 0xFF)) {
          DO_(::google::protobuf::internal::WireFormatLite::ReadString(
                input, this->mutable_message()));
          DO_(::google::protobuf::internal::WireFormatLite::VerifyUtf8String(
            this->message().data(), static_cast<int>(this->message().length()),
            ::google::protobuf::internal::WireFormatLite::PARSE,
            "google.rpc.Status.message"));
        } else {
          goto handle_unusual;
        }
        break;
      }

      // repeated .google.protobuf.Any details = 3;
      case 3: {
        if (static_cast< ::google::protobuf::uint8>(tag) == (26 & 0xFF)) {
          DO_(::google::protobuf::internal::WireFormatLite::ReadMessage(
                input, add_details()));
        } else {
          goto handle_unusual;
        }
        break;
      }

      default: {
      handle_unusual:
        if (tag == 0) {
          goto success;
        }
        DO_(::google::protobuf::internal::WireFormat::SkipField(
              input, tag, _internal_metadata_.mutable_unknown_fields()));
        break;
      }
    }
  }
success:
  // @@protoc_insertion_point(parse_success:google.rpc.Status)
  return true;
failure:
  // @@protoc_insertion_point(parse_failure:google.rpc.Status)
  return false;
#undef DO_
}
#endif  // GOOGLE_PROTOBUF_ENABLE_EXPERIMENTAL_PARSER

void Status::SerializeWithCachedSizes(
    ::google::protobuf::io::CodedOutputStream* output) const {
  // @@protoc_insertion_point(serialize_start:google.rpc.Status)
  ::google::protobuf::uint32 cached_has_bits = 0;
  (void) cached_has_bits;

  // int32 code = 1;
  if (this->code() != 0) {
    ::google::protobuf::internal::WireFormatLite::WriteInt32(1, this->code(), output);
  }

  // string message = 2;
  if (this->message().size() > 0) {
    ::google::protobuf::internal::WireFormatLite::VerifyUtf8String(
      this->message().data(), static_cast<int>(this->message().length()),
      ::google::protobuf::internal::WireFormatLite::SERIALIZE,
      "google.rpc.Status.message");
    ::google::protobuf::internal::WireFormatLite::WriteStringMaybeAliased(
      2, this->message(), output);
  }

  // repeated .google.protobuf.Any details = 3;
  for (unsigned int i = 0,
      n = static_cast<unsigned int>(this->details_size()); i < n; i++) {
    ::google::protobuf::internal::WireFormatLite::WriteMessageMaybeToArray(
      3,
      this->details(static_cast<int>(i)),
      output);
  }

  if (_internal_metadata_.have_unknown_fields()) {
    ::google::protobuf::internal::WireFormat::SerializeUnknownFields(
        _internal_metadata_.unknown_fields(), output);
  }
  // @@protoc_insertion_point(serialize_end:google.rpc.Status)
}

::google::protobuf::uint8* Status::InternalSerializeWithCachedSizesToArray(
    ::google::protobuf::uint8* target) const {
  // @@protoc_insertion_point(serialize_to_array_start:google.rpc.Status)
  ::google::protobuf::uint32 cached_has_bits = 0;
  (void) cached_has_bits;

  // int32 code = 1;
  if (this->code() != 0) {
    target = ::google::protobuf::internal::WireFormatLite::WriteInt32ToArray(1, this->code(), target);
  }

  // string message = 2;
  if (this->message().size() > 0) {
    ::google::protobuf::internal::WireFormatLite::VerifyUtf8String(
      this->message().data(), static_cast<int>(this->message().length()),
      ::google::protobuf::internal::WireFormatLite::SERIALIZE,
      "google.rpc.Status.message");
    target =
      ::google::protobuf::internal::WireFormatLite::WriteStringToArray(
        2, this->message(), target);
  }

  // repeated .google.protobuf.Any details = 3;
  for (unsigned int i = 0,
      n = static_cast<unsigned int>(this->details_size()); i < n; i++) {
    target = ::google::protobuf::internal::WireFormatLite::
      InternalWriteMessageToArray(
        3, this->details(static_cast<int>(i)), target);
  }

  if (_internal_metadata_.have_unknown_fields()) {
    target = ::google::protobuf::internal::WireFormat::SerializeUnknownFieldsToArray(
        _internal_metadata_.unknown_fields(), target);
  }
  // @@protoc_insertion_point(serialize_to_array_end:google.rpc.Status)
  return target;
}

size_t Status::ByteSizeLong() const {
// @@protoc_insertion_point(message_byte_size_start:google.rpc.Status)
  size_t total_size = 0;

  if (_internal_metadata_.have_unknown_fields()) {
    total_size +=
      ::google::protobuf::internal::WireFormat::ComputeUnknownFieldsSize(
        _internal_metadata_.unknown_fields());
  }
  ::google::protobuf::uint32 cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  // repeated .google.protobuf.Any details = 3;
  {
    unsigned int count = static_cast<unsigned int>(this->details_size());
    total_size += 1UL * count;
    for (unsigned int i = 0; i < count; i++) {
      total_size +=
        ::google::protobuf::internal::WireFormatLite::MessageSize(
          this->details(static_cast<int>(i)));
    }
  }

  // string message = 2;
  if (this->message().size() > 0) {
    total_size += 1 +
      ::google::protobuf::internal::WireFormatLite::StringSize(
        this->message());
  }

  // int32 code = 1;
  if (this->code() != 0) {
    total_size += 1 +
      ::google::protobuf::internal::WireFormatLite::Int32Size(
        this->code());
  }

  int cached_size = ::google::protobuf::internal::ToCachedSize(total_size);
  SetCachedSize(cached_size);
  return total_size;
}

void Status::MergeFrom(const ::google::protobuf::Message& from) {
// @@protoc_insertion_point(generalized_merge_from_start:google.rpc.Status)
  GOOGLE_DCHECK_NE(&from, this);
  const Status* source =
      ::google::protobuf::DynamicCastToGenerated<Status>(
          &from);
  if (source == nullptr) {
  // @@protoc_insertion_point(generalized_merge_from_cast_fail:google.rpc.Status)
    ::google::protobuf::internal::ReflectionOps::Merge(from, this);
  } else {
  // @@protoc_insertion_point(generalized_merge_from_cast_success:google.rpc.Status)
    MergeFrom(*source);
  }
}

void Status::MergeFrom(const Status& from) {
// @@protoc_insertion_point(class_specific_merge_from_start:google.rpc.Status)
  GOOGLE_DCHECK_NE(&from, this);
  _internal_metadata_.MergeFrom(from._internal_metadata_);
  ::google::protobuf::uint32 cached_has_bits = 0;
  (void) cached_has_bits;

  details_.MergeFrom(from.details_);
  if (from.message().size() > 0) {

    message_.AssignWithDefault(&::google::protobuf::internal::GetEmptyStringAlreadyInited(), from.message_);
  }
  if (from.code() != 0) {
    set_code(from.code());
  }
}

void Status::CopyFrom(const ::google::protobuf::Message& from) {
// @@protoc_insertion_point(generalized_copy_from_start:google.rpc.Status)
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

void Status::CopyFrom(const Status& from) {
// @@protoc_insertion_point(class_specific_copy_from_start:google.rpc.Status)
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

bool Status::IsInitialized() const {
  return true;
}

void Status::Swap(Status* other) {
  if (other == this) return;
  InternalSwap(other);
}
void Status::InternalSwap(Status* other) {
  using std::swap;
  _internal_metadata_.Swap(&other->_internal_metadata_);
  CastToBase(&details_)->InternalSwap(CastToBase(&other->details_));
  message_.Swap(&other->message_, &::google::protobuf::internal::GetEmptyStringAlreadyInited(),
    GetArenaNoVirtual());
  swap(code_, other->code_);
}

::google::protobuf::Metadata Status::GetMetadata() const {
  ::google::protobuf::internal::AssignDescriptors(&::assign_descriptors_table_google_2frpc_2fstatus_2eproto);
  return ::file_level_metadata_google_2frpc_2fstatus_2eproto[kIndexInFileMessages];
}


// @@protoc_insertion_point(namespace_scope)
}  // namespace rpc
}  // namespace google
namespace google {
namespace protobuf {
template<> PROTOBUF_NOINLINE ::google::rpc::Status* Arena::CreateMaybeMessage< ::google::rpc::Status >(Arena* arena) {
  return Arena::CreateInternal< ::google::rpc::Status >(arena);
}
}  // namespace protobuf
}  // namespace google

// @@protoc_insertion_point(global_scope)
#include <google/protobuf/port_undef.inc>
