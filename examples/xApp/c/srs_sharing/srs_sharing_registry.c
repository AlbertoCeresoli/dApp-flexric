/*
 * [srs-sharing] Registry of the SRS sharing xApp; see srs_sharing_registry.h.
 */
#include "srs_sharing_registry.h"

#include "srs_sharing.pb-c.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

/* Bounds: a lab deployment has a handful of UEs and reporters; beyond these, new entries are refused. */
#define MAX_UES 256
#define MAX_SOURCES 64

typedef SrsSharing__V1__SrsSharingMessage message_t;

typedef struct {
  char plmn[8]; /* PLMN-Identity digits, e.g. "00101" */
  uint64_t gnb_id;
  uint32_t phys_cell_id;
  uint32_t rnti;
} ue_key_t;

typedef struct {
  bool has_rar;
  int64_t n_ta_steps;
  uint32_t nof_commands;
  uint64_t last_command_timestamp;
  bool missed_command_suspected;
} ledger_t;

typedef struct {
  ue_key_t key;
  message_t* config;  /* owns the decoded message holding the latest SrsConfigReport; NULL until one arrives */
  ledger_t ledger;
  srs_source_t source;  /* reporter of the latest message about this UE */
} ue_entry_t;

typedef struct {
  srs_source_t src;
  uint64_t last_sequence;
} source_entry_t;

struct srs_registry {
  ue_entry_t ues[MAX_UES];
  size_t nof_ues;
  source_entry_t sources[MAX_SOURCES];
  size_t nof_sources;
  uint64_t sequence; /* of the updates this xApp sends */
  srs_registry_stats_t stats;
};

srs_registry_t* srs_registry_new(void)
{
  return calloc(1, sizeof(srs_registry_t));
}

void srs_registry_free(srs_registry_t* reg)
{
  if (reg == NULL)
    return;
  for (size_t i = 0; i < reg->nof_ues; ++i)
    if (reg->ues[i].config != NULL)
      srs_sharing__v1__srs_sharing_message__free_unpacked(reg->ues[i].config, NULL);
  free(reg);
}

size_t srs_registry_nof_ues(srs_registry_t const* reg)
{
  return reg->nof_ues;
}

srs_registry_stats_t srs_registry_stats(srs_registry_t const* reg)
{
  return reg->stats;
}

static bool same_source(srs_source_t const* a, srs_source_t const* b)
{
  return memcmp(a->plmn, b->plmn, 3) == 0 && a->nb_id == b->nb_id && a->dapp_id == b->dapp_id;
}

static bool key_of(SrsSharing__V1__UeRef const* ue, ue_key_t* key)
{
  if (ue == NULL || ue->serving_cell == NULL || ue->serving_cell->plmn_identity == NULL)
    return false;
  memset(key, 0, sizeof(*key));
  strncpy(key->plmn, ue->serving_cell->plmn_identity, sizeof(key->plmn) - 1);
  key->gnb_id = ue->serving_cell->gnb_id;
  key->phys_cell_id = ue->serving_cell->phys_cell_id;
  key->rnti = ue->rnti;
  return true;
}

static bool same_key(ue_key_t const* a, ue_key_t const* b)
{
  return strcmp(a->plmn, b->plmn) == 0 && a->gnb_id == b->gnb_id && a->phys_cell_id == b->phys_cell_id
         && a->rnti == b->rnti;
}

static ue_entry_t* find_ue(srs_registry_t* reg, ue_key_t const* key, bool create)
{
  for (size_t i = 0; i < reg->nof_ues; ++i)
    if (same_key(&reg->ues[i].key, key))
      return &reg->ues[i];
  if (!create || reg->nof_ues == MAX_UES)
    return NULL;
  ue_entry_t* e = &reg->ues[reg->nof_ues++];
  memset(e, 0, sizeof(*e));
  e->key = *key;
  return e;
}

static void remove_ue(srs_registry_t* reg, ue_entry_t* e)
{
  if (e->config != NULL)
    srs_sharing__v1__srs_sharing_message__free_unpacked(e->config, NULL);
  size_t idx = (size_t)(e - reg->ues);
  reg->ues[idx] = reg->ues[--reg->nof_ues];
}

/* Sequence check per reporter. Returns true when messages were lost since the previous one. A sequence that goes
 * back is a restarted reporter: it starts over without a gap. */
static bool sequence_gap(srs_registry_t* reg, srs_source_t src, uint64_t sequence)
{
  for (size_t i = 0; i < reg->nof_sources; ++i) {
    source_entry_t* s = &reg->sources[i];
    if (!same_source(&s->src, &src))
      continue;
    bool gap = sequence > s->last_sequence + 1;
    s->last_sequence = sequence;
    return gap;
  }
  if (reg->nof_sources < MAX_SOURCES)
    reg->sources[reg->nof_sources++] = (source_entry_t){.src = src, .last_sequence = sequence};
  return false;
}

/* Wraps a ForeignUeUpdate in an SrsSharingMessage and packs it. */
static srs_packed_t pack_update(srs_registry_t* reg, SrsSharing__V1__ForeignUeUpdate* update, uint64_t now_us)
{
  SrsSharing__V1__Header header = SRS_SHARING__V1__HEADER__INIT;
  header.schema_version = SRS_SHARING__V1__SCHEMA_VERSION__SCHEMA_VERSION_1;
  header.sequence = ++reg->sequence;
  header.timestamp = now_us;

  message_t msg = SRS_SHARING__V1__SRS_SHARING_MESSAGE__INIT;
  msg.header = &header;
  msg.body_case = SRS_SHARING__V1__SRS_SHARING_MESSAGE__BODY_FOREIGN_UE_UPDATE;
  msg.foreign_ue_update = update;

  srs_packed_t out = {.len = srs_sharing__v1__srs_sharing_message__get_packed_size(&msg)};
  out.buf = malloc(out.len > 0 ? out.len : 1);
  assert(out.buf != NULL);
  srs_sharing__v1__srs_sharing_message__pack(&msg, out.buf);
  reg->stats.updates++;
  return out;
}

/* The current state of a UE with a known resource. The update points into the stored report; nothing is copied. */
static srs_packed_t pack_ue(srs_registry_t* reg, ue_entry_t const* e, uint64_t now_us)
{
  SrsSharing__V1__SrsConfigReport const* report = e->config->srs_config_report;

  SrsSharing__V1__TimingAdvanceState ta = SRS_SHARING__V1__TIMING_ADVANCE_STATE__INIT;
  ta.n_ta_steps = e->ledger.n_ta_steps;
  ta.nof_commands = e->ledger.nof_commands;
  ta.last_command_timestamp = e->ledger.last_command_timestamp;
  ta.missed_command_suspected = e->ledger.missed_command_suspected;

  SrsSharing__V1__ForeignUeUpdate update = SRS_SHARING__V1__FOREIGN_UE_UPDATE__INIT;
  update.ue = report->ue;
  update.config_epoch = report->config_epoch;
  update.uplink = report->uplink;
  update.resource = report->resource;
  update.anchor = report->anchor;
  /* Without any command the ledger says nothing: leave it out rather than send a zero TA. */
  update.timing_advance = e->ledger.nof_commands > 0 ? &ta : NULL;
  return pack_update(reg, &update, now_us);
}

static bool has_resource(ue_entry_t const* e)
{
  return e->config != NULL && e->config->srs_config_report->resource != NULL;
}

/* Returns the number of updates written; takes ownership of msg. */
static size_t on_config(srs_registry_t* reg, srs_source_t src, message_t* msg, uint64_t now_us, srs_packed_t* out,
                        size_t max_out)
{
  SrsSharing__V1__SrsConfigReport const* report = msg->srs_config_report;
  ue_key_t key;
  if (!key_of(report->ue, &key)) {
    reg->stats.invalid++;
    srs_sharing__v1__srs_sharing_message__free_unpacked(msg, NULL);
    return 0;
  }

  if (report->event == SRS_SHARING__V1__CONFIG_EVENT__CONFIG_EVENT_REMOVED) {
    size_t n = 0;
    if (max_out > 0) {
      SrsSharing__V1__ForeignUeUpdate update = SRS_SHARING__V1__FOREIGN_UE_UPDATE__INIT;
      update.ue = report->ue;
      update.config_epoch = report->config_epoch;
      update.released = true;
      out[n++] = pack_update(reg, &update, now_us);
    }
    ue_entry_t* e = find_ue(reg, &key, false);
    if (e != NULL)
      remove_ue(reg, e);
    srs_sharing__v1__srs_sharing_message__free_unpacked(msg, NULL);
    return n;
  }

  ue_entry_t* e = find_ue(reg, &key, true);
  if (e == NULL) {
    srs_sharing__v1__srs_sharing_message__free_unpacked(msg, NULL);
    return 0;
  }
  if (e->config != NULL)
    srs_sharing__v1__srs_sharing_message__free_unpacked(e->config, NULL);
  e->config = msg;
  e->source = src;
  if (!has_resource(e) || max_out == 0)
    return 0;
  out[0] = pack_ue(reg, e, now_us);
  return 1;
}

static size_t on_timing_advance(srs_registry_t* reg, srs_source_t src, message_t* msg, uint64_t now_us,
                                srs_packed_t* out, size_t max_out)
{
  SrsSharing__V1__TimingAdvanceIndicationMessage const* ind = msg->timing_advance_indication;
  size_t n = 0;
  for (size_t i = 0; i < ind->n_timing_advance_data; ++i) {
    SrsSharing__V1__TimingAdvanceData const* d = ind->timing_advance_data[i];
    ue_key_t key;
    if (!key_of(d->ue, &key)) {
      reg->stats.invalid++;
      continue;
    }
    ue_entry_t* e = find_ue(reg, &key, true);
    if (e == NULL)
      continue;
    ledger_t* l = &e->ledger;
    if (d->source == SRS_SHARING__V1__TIMING_ADVANCE_SOURCE__TIMING_ADVANCE_SOURCE_RAR) {
      /* Absolute: restarts the ledger and clears earlier doubts. */
      l->has_rar = true;
      l->n_ta_steps = d->timing_advance;
      l->nof_commands = 1;
      l->missed_command_suspected = false;
    } else {
      l->n_ta_steps += (int64_t)d->timing_advance - 31;
      l->nof_commands++;
      if (!l->has_rar)
        l->missed_command_suspected = true;
    }
    l->last_command_timestamp = d->timestamp;
    e->source = src;
    if (has_resource(e) && n < max_out)
      out[n++] = pack_ue(reg, e, now_us);
  }
  srs_sharing__v1__srs_sharing_message__free_unpacked(msg, NULL);
  return n;
}

size_t srs_registry_on_payload(srs_registry_t* reg, srs_source_t src, uint8_t const* data, size_t len,
                               uint64_t now_us, srs_packed_t* out, size_t max_out)
{
  message_t* msg = srs_sharing__v1__srs_sharing_message__unpack(NULL, len, data);
  if (msg == NULL || msg->header == NULL
      || msg->header->schema_version != SRS_SHARING__V1__SCHEMA_VERSION__SCHEMA_VERSION_1) {
    reg->stats.invalid++;
    if (msg != NULL)
      srs_sharing__v1__srs_sharing_message__free_unpacked(msg, NULL);
    return 0;
  }

  if (sequence_gap(reg, src, msg->header->sequence)) {
    /* Messages from this reporter were lost: any of them may have been a TA command of its UEs. */
    reg->stats.sequence_gaps++;
    for (size_t i = 0; i < reg->nof_ues; ++i)
      if (same_source(&reg->ues[i].source, &src))
        reg->ues[i].ledger.missed_command_suspected = true;
  }

  switch (msg->body_case) {
    case SRS_SHARING__V1__SRS_SHARING_MESSAGE__BODY_SRS_CONFIG_REPORT:
      reg->stats.messages++;
      return on_config(reg, src, msg, now_us, out, max_out);
    case SRS_SHARING__V1__SRS_SHARING_MESSAGE__BODY_TIMING_ADVANCE_INDICATION:
      reg->stats.messages++;
      return on_timing_advance(reg, src, msg, now_us, out, max_out);
    default:
      /* No body, or a ForeignUeUpdate (only the xApp sends those). */
      reg->stats.invalid++;
      srs_sharing__v1__srs_sharing_message__free_unpacked(msg, NULL);
      return 0;
  }
}

size_t srs_registry_snapshot(srs_registry_t* reg, uint64_t now_us, srs_packed_t* out, size_t max_out)
{
  size_t n = 0;
  for (size_t i = 0; i < reg->nof_ues && n < max_out; ++i)
    if (has_resource(&reg->ues[i]))
      out[n++] = pack_ue(reg, &reg->ues[i], now_us);
  return n;
}
