// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: envoy/service/discovery/v2/ads.proto

#include "envoy/service/discovery/v2/ads.pb.h"

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

namespace envoy {
namespace service {
namespace discovery {
namespace v2 {
class AdsDummyDefaultTypeInternal {
 public:
  ::google::protobuf::internal::ExplicitlyConstructed<AdsDummy> _instance;
} _AdsDummy_default_instance_;
}  // namespace v2
}  // namespace discovery
}  // namespace service
}  // namespace envoy
static void InitDefaultsAdsDummy_envoy_2fservice_2fdiscovery_2fv2_2fads_2eproto() {
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  {
    void* ptr = &::envoy::service::discovery::v2::_AdsDummy_default_instance_;
    new (ptr) ::envoy::service::discovery::v2::AdsDummy();
    ::google::protobuf::internal::OnShutdownDestroyMessage(ptr);
  }
  ::envoy::service::discovery::v2::AdsDummy::InitAsDefaultInstance();
}

::google::protobuf::internal::SCCInfo<0> scc_info_AdsDummy_envoy_2fservice_2fdiscovery_2fv2_2fads_2eproto =
    {{ATOMIC_VAR_INIT(::google::protobuf::internal::SCCInfoBase::kUninitialized), 0, InitDefaultsAdsDummy_envoy_2fservice_2fdiscovery_2fv2_2fads_2eproto}, {}};

void InitDefaults_envoy_2fservice_2fdiscovery_2fv2_2fads_2eproto() {
  ::google::protobuf::internal::InitSCC(&scc_info_AdsDummy_envoy_2fservice_2fdiscovery_2fv2_2fads_2eproto.base);
}

::google::protobuf::Metadata file_level_metadata_envoy_2fservice_2fdiscovery_2fv2_2fads_2eproto[1];
constexpr ::google::protobuf::EnumDescriptor const** file_level_enum_descriptors_envoy_2fservice_2fdiscovery_2fv2_2fads_2eproto = nullptr;
constexpr ::google::protobuf::ServiceDescriptor const** file_level_service_descriptors_envoy_2fservice_2fdiscovery_2fv2_2fads_2eproto = nullptr;

const ::google::protobuf::uint32 TableStruct_envoy_2fservice_2fdiscovery_2fv2_2fads_2eproto::offsets[] PROTOBUF_SECTION_VARIABLE(protodesc_cold) = {
  ~0u,  // no _has_bits_
  PROTOBUF_FIELD_OFFSET(::envoy::service::discovery::v2::AdsDummy, _internal_metadata_),
  ~0u,  // no _extensions_
  ~0u,  // no _oneof_case_
  ~0u,  // no _weak_field_map_
};
static const ::google::protobuf::internal::MigrationSchema schemas[] PROTOBUF_SECTION_VARIABLE(protodesc_cold) = {
  { 0, -1, sizeof(::envoy::service::discovery::v2::AdsDummy)},
};

static ::google::protobuf::Message const * const file_default_instances[] = {
  reinterpret_cast<const ::google::protobuf::Message*>(&::envoy::service::discovery::v2::_AdsDummy_default_instance_),
};

::google::protobuf::internal::AssignDescriptorsTable assign_descriptors_table_envoy_2fservice_2fdiscovery_2fv2_2fads_2eproto = {
  {}, AddDescriptors_envoy_2fservice_2fdiscovery_2fv2_2fads_2eproto, "envoy/service/discovery/v2/ads.proto", schemas,
  file_default_instances, TableStruct_envoy_2fservice_2fdiscovery_2fv2_2fads_2eproto::offsets,
  file_level_metadata_envoy_2fservice_2fdiscovery_2fv2_2fads_2eproto, 1, file_level_enum_descriptors_envoy_2fservice_2fdiscovery_2fv2_2fads_2eproto, file_level_service_descriptors_envoy_2fservice_2fdiscovery_2fv2_2fads_2eproto,
};

const char descriptor_table_protodef_envoy_2fservice_2fdiscovery_2fv2_2fads_2eproto[] =
  "\n$envoy/service/discovery/v2/ads.proto\022\032"
  "envoy.service.discovery.v2\032\034envoy/api/v2"
  "/discovery.proto\"\n\n\010AdsDummy2\377\001\n\032Aggrega"
  "tedDiscoveryService\022b\n\031StreamAggregatedR"
  "esources\022\036.envoy.api.v2.DiscoveryRequest"
  "\032\037.envoy.api.v2.DiscoveryResponse\"\000(\0010\001\022"
  "}\n\036IncrementalAggregatedResources\022).envo"
  "y.api.v2.IncrementalDiscoveryRequest\032*.e"
  "nvoy.api.v2.IncrementalDiscoveryResponse"
  "\"\000(\0010\001B\007Z\002v2\210\001\001b\006proto3"
  ;
::google::protobuf::internal::DescriptorTable descriptor_table_envoy_2fservice_2fdiscovery_2fv2_2fads_2eproto = {
  false, InitDefaults_envoy_2fservice_2fdiscovery_2fv2_2fads_2eproto, 
  descriptor_table_protodef_envoy_2fservice_2fdiscovery_2fv2_2fads_2eproto,
  "envoy/service/discovery/v2/ads.proto", &assign_descriptors_table_envoy_2fservice_2fdiscovery_2fv2_2fads_2eproto, 383,
};

void AddDescriptors_envoy_2fservice_2fdiscovery_2fv2_2fads_2eproto() {
  static constexpr ::google::protobuf::internal::InitFunc deps[1] =
  {
    ::AddDescriptors_envoy_2fapi_2fv2_2fdiscovery_2eproto,
  };
 ::google::protobuf::internal::AddDescriptors(&descriptor_table_envoy_2fservice_2fdiscovery_2fv2_2fads_2eproto, deps, 1);
}

// Force running AddDescriptors() at dynamic initialization time.
static bool dynamic_init_dummy_envoy_2fservice_2fdiscovery_2fv2_2fads_2eproto = []() { AddDescriptors_envoy_2fservice_2fdiscovery_2fv2_2fads_2eproto(); return true; }();
namespace envoy {
namespace service {
namespace discovery {
namespace v2 {

// ===================================================================

void AdsDummy::InitAsDefaultInstance() {
}
class AdsDummy::HasBitSetters {
 public:
};

#if !defined(_MSC_VER) || _MSC_VER >= 1900
#endif  // !defined(_MSC_VER) || _MSC_VER >= 1900

AdsDummy::AdsDummy()
  : ::google::protobuf::Message(), _internal_metadata_(nullptr) {
  SharedCtor();
  // @@protoc_insertion_point(constructor:envoy.service.discovery.v2.AdsDummy)
}
AdsDummy::AdsDummy(const AdsDummy& from)
  : ::google::protobuf::Message(),
      _internal_metadata_(nullptr) {
  _internal_metadata_.MergeFrom(from._internal_metadata_);
  // @@protoc_insertion_point(copy_constructor:envoy.service.discovery.v2.AdsDummy)
}

void AdsDummy::SharedCtor() {
}

AdsDummy::~AdsDummy() {
  // @@protoc_insertion_point(destructor:envoy.service.discovery.v2.AdsDummy)
  SharedDtor();
}

void AdsDummy::SharedDtor() {
}

void AdsDummy::SetCachedSize(int size) const {
  _cached_size_.Set(size);
}
const AdsDummy& AdsDummy::default_instance() {
  ::google::protobuf::internal::InitSCC(&::scc_info_AdsDummy_envoy_2fservice_2fdiscovery_2fv2_2fads_2eproto.base);
  return *internal_default_instance();
}


void AdsDummy::Clear() {
// @@protoc_insertion_point(message_clear_start:envoy.service.discovery.v2.AdsDummy)
  ::google::protobuf::uint32 cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  _internal_metadata_.Clear();
}

#if GOOGLE_PROTOBUF_ENABLE_EXPERIMENTAL_PARSER
const char* AdsDummy::_InternalParse(const char* begin, const char* end, void* object,
                  ::google::protobuf::internal::ParseContext* ctx) {
  auto msg = static_cast<AdsDummy*>(object);
  ::google::protobuf::int32 size; (void)size;
  int depth; (void)depth;
  ::google::protobuf::uint32 tag;
  ::google::protobuf::internal::ParseFunc parser_till_end; (void)parser_till_end;
  auto ptr = begin;
  while (ptr < end) {
    ptr = ::google::protobuf::io::Parse32(ptr, &tag);
    GOOGLE_PROTOBUF_PARSER_ASSERT(ptr);
    switch (tag >> 3) {
      default: {
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
}
#else  // GOOGLE_PROTOBUF_ENABLE_EXPERIMENTAL_PARSER
bool AdsDummy::MergePartialFromCodedStream(
    ::google::protobuf::io::CodedInputStream* input) {
#define DO_(EXPRESSION) if (!PROTOBUF_PREDICT_TRUE(EXPRESSION)) goto failure
  ::google::protobuf::uint32 tag;
  // @@protoc_insertion_point(parse_start:envoy.service.discovery.v2.AdsDummy)
  for (;;) {
    ::std::pair<::google::protobuf::uint32, bool> p = input->ReadTagWithCutoffNoLastTag(127u);
    tag = p.first;
    if (!p.second) goto handle_unusual;
  handle_unusual:
    if (tag == 0) {
      goto success;
    }
    DO_(::google::protobuf::internal::WireFormat::SkipField(
          input, tag, _internal_metadata_.mutable_unknown_fields()));
  }
success:
  // @@protoc_insertion_point(parse_success:envoy.service.discovery.v2.AdsDummy)
  return true;
failure:
  // @@protoc_insertion_point(parse_failure:envoy.service.discovery.v2.AdsDummy)
  return false;
#undef DO_
}
#endif  // GOOGLE_PROTOBUF_ENABLE_EXPERIMENTAL_PARSER

void AdsDummy::SerializeWithCachedSizes(
    ::google::protobuf::io::CodedOutputStream* output) const {
  // @@protoc_insertion_point(serialize_start:envoy.service.discovery.v2.AdsDummy)
  ::google::protobuf::uint32 cached_has_bits = 0;
  (void) cached_has_bits;

  if (_internal_metadata_.have_unknown_fields()) {
    ::google::protobuf::internal::WireFormat::SerializeUnknownFields(
        _internal_metadata_.unknown_fields(), output);
  }
  // @@protoc_insertion_point(serialize_end:envoy.service.discovery.v2.AdsDummy)
}

::google::protobuf::uint8* AdsDummy::InternalSerializeWithCachedSizesToArray(
    ::google::protobuf::uint8* target) const {
  // @@protoc_insertion_point(serialize_to_array_start:envoy.service.discovery.v2.AdsDummy)
  ::google::protobuf::uint32 cached_has_bits = 0;
  (void) cached_has_bits;

  if (_internal_metadata_.have_unknown_fields()) {
    target = ::google::protobuf::internal::WireFormat::SerializeUnknownFieldsToArray(
        _internal_metadata_.unknown_fields(), target);
  }
  // @@protoc_insertion_point(serialize_to_array_end:envoy.service.discovery.v2.AdsDummy)
  return target;
}

size_t AdsDummy::ByteSizeLong() const {
// @@protoc_insertion_point(message_byte_size_start:envoy.service.discovery.v2.AdsDummy)
  size_t total_size = 0;

  if (_internal_metadata_.have_unknown_fields()) {
    total_size +=
      ::google::protobuf::internal::WireFormat::ComputeUnknownFieldsSize(
        _internal_metadata_.unknown_fields());
  }
  ::google::protobuf::uint32 cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  int cached_size = ::google::protobuf::internal::ToCachedSize(total_size);
  SetCachedSize(cached_size);
  return total_size;
}

void AdsDummy::MergeFrom(const ::google::protobuf::Message& from) {
// @@protoc_insertion_point(generalized_merge_from_start:envoy.service.discovery.v2.AdsDummy)
  GOOGLE_DCHECK_NE(&from, this);
  const AdsDummy* source =
      ::google::protobuf::DynamicCastToGenerated<AdsDummy>(
          &from);
  if (source == nullptr) {
  // @@protoc_insertion_point(generalized_merge_from_cast_fail:envoy.service.discovery.v2.AdsDummy)
    ::google::protobuf::internal::ReflectionOps::Merge(from, this);
  } else {
  // @@protoc_insertion_point(generalized_merge_from_cast_success:envoy.service.discovery.v2.AdsDummy)
    MergeFrom(*source);
  }
}

void AdsDummy::MergeFrom(const AdsDummy& from) {
// @@protoc_insertion_point(class_specific_merge_from_start:envoy.service.discovery.v2.AdsDummy)
  GOOGLE_DCHECK_NE(&from, this);
  _internal_metadata_.MergeFrom(from._internal_metadata_);
  ::google::protobuf::uint32 cached_has_bits = 0;
  (void) cached_has_bits;

}

void AdsDummy::CopyFrom(const ::google::protobuf::Message& from) {
// @@protoc_insertion_point(generalized_copy_from_start:envoy.service.discovery.v2.AdsDummy)
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

void AdsDummy::CopyFrom(const AdsDummy& from) {
// @@protoc_insertion_point(class_specific_copy_from_start:envoy.service.discovery.v2.AdsDummy)
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

bool AdsDummy::IsInitialized() const {
  return true;
}

void AdsDummy::Swap(AdsDummy* other) {
  if (other == this) return;
  InternalSwap(other);
}
void AdsDummy::InternalSwap(AdsDummy* other) {
  using std::swap;
  _internal_metadata_.Swap(&other->_internal_metadata_);
}

::google::protobuf::Metadata AdsDummy::GetMetadata() const {
  ::google::protobuf::internal::AssignDescriptors(&::assign_descriptors_table_envoy_2fservice_2fdiscovery_2fv2_2fads_2eproto);
  return ::file_level_metadata_envoy_2fservice_2fdiscovery_2fv2_2fads_2eproto[kIndexInFileMessages];
}


// @@protoc_insertion_point(namespace_scope)
}  // namespace v2
}  // namespace discovery
}  // namespace service
}  // namespace envoy
namespace google {
namespace protobuf {
template<> PROTOBUF_NOINLINE ::envoy::service::discovery::v2::AdsDummy* Arena::CreateMaybeMessage< ::envoy::service::discovery::v2::AdsDummy >(Arena* arena) {
  return Arena::CreateInternal< ::envoy::service::discovery::v2::AdsDummy >(arena);
}
}  // namespace protobuf
}  // namespace google

// @@protoc_insertion_point(global_scope)
#include <google/protobuf/port_undef.inc>
