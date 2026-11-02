/*
 * [srs-sharing] Tests of the SRS sharing xApp registry (srs_sharing_registry.c), with packed protobuf-c messages as
 * a reporting dApp sends them.
 */
#include "srs_sharing_registry.h"

#include "srs_sharing.pb-c.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static srs_source_t const SRC_A = {.plmn = {0x00, 0xf1, 0x10}, .nb_id = 411, .dapp_id = 101};

/* ---- building reporter messages -------------------------------------------------------------------------- */

/* A packed payload; builders return buffer and length together (C leaves argument evaluation order unspecified,
 * so a builder writing the length through a pointer could not be called in the same expression that reads it). */
typedef struct {
  uint8_t* buf;
  size_t len;
} payload_t;

typedef struct {
  SrsSharing__V1__CellRef cell;
  SrsSharing__V1__UeRef ue;
} ue_ref_t;

static void make_ue(ue_ref_t* r, uint32_t rnti)
{
  srs_sharing__v1__cell_ref__init(&r->cell);
  r->cell.plmn_identity = "00101";
  r->cell.gnb_id = 411;
  r->cell.gnb_id_length = 22;
  r->cell.phys_cell_id = 1;
  srs_sharing__v1__ue_ref__init(&r->ue);
  r->ue.serving_cell = &r->cell;
  r->ue.rnti = rnti;
}

static uint8_t* pack(SrsSharing__V1__SrsSharingMessage* m, uint64_t sequence, uint32_t version, size_t* len)
{
  SrsSharing__V1__Header h = SRS_SHARING__V1__HEADER__INIT;
  h.schema_version = version;
  h.sequence = sequence;
  h.timestamp = 1;
  m->header = &h;
  *len = srs_sharing__v1__srs_sharing_message__get_packed_size(m);
  uint8_t* buf = malloc(*len + 1);
  srs_sharing__v1__srs_sharing_message__pack(m, buf);
  m->header = NULL;
  return buf;
}

/* SrsConfigReport for rnti, with or without a resource (period 20, offset `offset`) and anchor. */
static payload_t config_report(uint32_t rnti, uint64_t epoch, int event, bool with_resource, uint32_t offset,
                               uint64_t sequence)
{
  ue_ref_t u;
  make_ue(&u, rnti);
  SrsSharing__V1__PeriodicityAndOffset po = SRS_SHARING__V1__PERIODICITY_AND_OFFSET__INIT;
  po.period = 20;
  po.offset = offset;
  SrsSharing__V1__SrsResource res = SRS_SHARING__V1__SRS_RESOURCE__INIT;
  res.nrof_srs_ports = 2;
  res.periodicity_and_offset = &po;
  SrsSharing__V1__SlotAnchor anchor = SRS_SHARING__V1__SLOT_ANCHOR__INIT;
  anchor.sfn = 100;
  anchor.slot = offset;
  anchor.timestamp = 5000000;
  SrsSharing__V1__SrsConfigReport r = SRS_SHARING__V1__SRS_CONFIG_REPORT__INIT;
  r.ue = &u.ue;
  r.config_epoch = epoch;
  r.event = event;
  if (with_resource) {
    r.resource = &res;
    r.anchor = &anchor;
  }
  SrsSharing__V1__SrsSharingMessage m = SRS_SHARING__V1__SRS_SHARING_MESSAGE__INIT;
  m.body_case = SRS_SHARING__V1__SRS_SHARING_MESSAGE__BODY_SRS_CONFIG_REPORT;
  m.srs_config_report = &r;
  payload_t p;
  p.buf = pack(&m, sequence, 1, &p.len);
  return p;
}

static payload_t ta_report(uint32_t rnti, int source, uint32_t value, uint64_t timestamp, uint64_t sequence)
{
  ue_ref_t u;
  make_ue(&u, rnti);
  SrsSharing__V1__TimingAdvanceData d = SRS_SHARING__V1__TIMING_ADVANCE_DATA__INIT;
  d.ue = &u.ue;
  d.source = source;
  d.timing_advance = value;
  d.timestamp = timestamp;
  SrsSharing__V1__TimingAdvanceData* list[] = {&d};
  SrsSharing__V1__TimingAdvanceIndicationMessage ind = SRS_SHARING__V1__TIMING_ADVANCE_INDICATION_MESSAGE__INIT;
  ind.n_timing_advance_data = 1;
  ind.timing_advance_data = list;
  SrsSharing__V1__SrsSharingMessage m = SRS_SHARING__V1__SRS_SHARING_MESSAGE__INIT;
  m.body_case = SRS_SHARING__V1__SRS_SHARING_MESSAGE__BODY_TIMING_ADVANCE_INDICATION;
  m.timing_advance_indication = &ind;
  payload_t p;
  p.buf = pack(&m, sequence, 1, &p.len);
  return p;
}

/* Feeds a payload; returns the number of updates and leaves the last one decoded in *update (or NULL). */
static size_t feed(srs_registry_t* reg, payload_t p, SrsSharing__V1__SrsSharingMessage** update)
{
  srs_packed_t out[8];
  size_t n = srs_registry_on_payload(reg, SRC_A, p.buf, p.len, 42, out, 8);
  free(p.buf);
  *update = NULL;
  for (size_t i = 0; i < n; ++i) {
    if (i == n - 1)
      *update = srs_sharing__v1__srs_sharing_message__unpack(NULL, out[i].len, out[i].buf);
    free(out[i].buf);
  }
  return n;
}

static void drop(SrsSharing__V1__SrsSharingMessage* m)
{
  if (m != NULL)
    srs_sharing__v1__srs_sharing_message__free_unpacked(m, NULL);
}

static int const RAR = SRS_SHARING__V1__TIMING_ADVANCE_SOURCE__TIMING_ADVANCE_SOURCE_RAR;
static int const MAC_CE = SRS_SHARING__V1__TIMING_ADVANCE_SOURCE__TIMING_ADVANCE_SOURCE_CONNECTED_UE;
static int const CREATED = SRS_SHARING__V1__CONFIG_EVENT__CONFIG_EVENT_CREATED;
static int const REMOVED = SRS_SHARING__V1__CONFIG_EVENT__CONFIG_EVENT_REMOVED;

/* ---- tests --------------------------------------------------------------------------------------------- */

static void test_config_produces_an_update(void)
{
  srs_registry_t* reg = srs_registry_new();
  SrsSharing__V1__SrsSharingMessage* u;
  assert(feed(reg, config_report(0x4601, 3, CREATED, true, 7, 1), &u) == 1);
  assert(u->body_case == SRS_SHARING__V1__SRS_SHARING_MESSAGE__BODY_FOREIGN_UE_UPDATE);
  assert(u->header->schema_version == 1 && u->header->sequence == 1 && u->header->timestamp == 42);
  SrsSharing__V1__ForeignUeUpdate const* f = u->foreign_ue_update;
  assert(f->ue->rnti == 0x4601 && f->config_epoch == 3 && !f->released);
  assert(strcmp(f->ue->serving_cell->plmn_identity, "00101") == 0 && f->ue->serving_cell->gnb_id == 411);
  assert(f->resource->periodicity_and_offset->offset == 7 && f->anchor->sfn == 100);
  assert(f->timing_advance == NULL); /* no command known yet */
  drop(u);
  assert(srs_registry_nof_ues(reg) == 1);
  srs_registry_free(reg);
}

static void test_config_without_resource_is_kept_silent(void)
{
  srs_registry_t* reg = srs_registry_new();
  SrsSharing__V1__SrsSharingMessage* u;
  assert(feed(reg, config_report(0x4601, 1, CREATED, false, 0, 1), &u) == 0);
  assert(srs_registry_nof_ues(reg) == 1);
  srs_registry_free(reg);
}

static void test_ledger_rar_then_mac_ce(void)
{
  srs_registry_t* reg = srs_registry_new();
  SrsSharing__V1__SrsSharingMessage* u;
  /* The RAR comes before the configuration: kept, no update yet. */
  assert(feed(reg, ta_report(0x4601, RAR, 17, 1000, 1), &u) == 0);
  assert(feed(reg, config_report(0x4601, 3, CREATED, true, 7, 2), &u) == 1);
  assert(u->foreign_ue_update->timing_advance->n_ta_steps == 17);
  assert(u->foreign_ue_update->timing_advance->nof_commands == 1);
  drop(u);
  /* MAC CE 33 = +2 steps, then 29 = -2, then 25 = -6: 17 + 2 - 2 - 6 = 11. */
  assert(feed(reg, ta_report(0x4601, MAC_CE, 33, 2000, 3), &u) == 1);
  drop(u);
  assert(feed(reg, ta_report(0x4601, MAC_CE, 29, 3000, 4), &u) == 1);
  drop(u);
  assert(feed(reg, ta_report(0x4601, MAC_CE, 25, 4000, 5), &u) == 1);
  SrsSharing__V1__TimingAdvanceState const* ta = u->foreign_ue_update->timing_advance;
  assert(ta->n_ta_steps == 11 && ta->nof_commands == 4 && ta->last_command_timestamp == 4000);
  assert(!ta->missed_command_suspected);
  drop(u);
  srs_registry_free(reg);
}

static void test_ledger_can_go_negative(void)
{
  srs_registry_t* reg = srs_registry_new();
  SrsSharing__V1__SrsSharingMessage* u;
  feed(reg, ta_report(0x4601, RAR, 1, 1000, 1), &u);
  feed(reg, config_report(0x4601, 3, CREATED, true, 7, 2), &u);
  drop(u);
  assert(feed(reg, ta_report(0x4601, MAC_CE, 0, 2000, 3), &u) == 1); /* 1 + (0 - 31) = -30 */
  assert(u->foreign_ue_update->timing_advance->n_ta_steps == -30);
  drop(u);
  srs_registry_free(reg);
}

static void test_mac_ce_without_rar_is_suspect_until_the_next_rar(void)
{
  srs_registry_t* reg = srs_registry_new();
  SrsSharing__V1__SrsSharingMessage* u;
  feed(reg, config_report(0x4601, 3, CREATED, true, 7, 1), &u);
  drop(u);
  assert(feed(reg, ta_report(0x4601, MAC_CE, 32, 2000, 2), &u) == 1);
  assert(u->foreign_ue_update->timing_advance->missed_command_suspected);
  drop(u);
  assert(feed(reg, ta_report(0x4601, RAR, 20, 3000, 3), &u) == 1);
  assert(!u->foreign_ue_update->timing_advance->missed_command_suspected);
  assert(u->foreign_ue_update->timing_advance->n_ta_steps == 20);
  drop(u);
  srs_registry_free(reg);
}

static void test_sequence_gap_marks_the_reporters_ues(void)
{
  srs_registry_t* reg = srs_registry_new();
  SrsSharing__V1__SrsSharingMessage* u;
  feed(reg, ta_report(0x4601, RAR, 17, 1000, 1), &u);
  feed(reg, config_report(0x4601, 3, CREATED, true, 7, 2), &u);
  assert(!u->foreign_ue_update->timing_advance->missed_command_suspected);
  drop(u);
  /* Sequences 3 and 4 never arrived. */
  assert(feed(reg, config_report(0x4601, 3, CREATED, true, 7, 5), &u) == 1);
  assert(u->foreign_ue_update->timing_advance->missed_command_suspected);
  drop(u);
  assert(srs_registry_stats(reg).sequence_gaps == 1);
  /* A reporter that restarts begins again at 1: not a gap. */
  feed(reg, config_report(0x4601, 3, CREATED, true, 7, 1), &u);
  drop(u);
  assert(srs_registry_stats(reg).sequence_gaps == 1);
  srs_registry_free(reg);
}

static void test_new_epoch_replaces_and_removal_releases(void)
{
  srs_registry_t* reg = srs_registry_new();
  SrsSharing__V1__SrsSharingMessage* u;
  feed(reg, config_report(0x4601, 3, CREATED, true, 7, 1), &u);
  drop(u);
  assert(feed(reg, config_report(0x4601, 4, CREATED, true, 9, 2), &u) == 1);
  assert(u->foreign_ue_update->config_epoch == 4);
  assert(u->foreign_ue_update->resource->periodicity_and_offset->offset == 9);
  drop(u);
  assert(feed(reg, config_report(0x4601, 5, REMOVED, false, 0, 3), &u) == 1);
  assert(u->foreign_ue_update->released && u->foreign_ue_update->config_epoch == 5);
  assert(u->foreign_ue_update->resource == NULL);
  drop(u);
  assert(srs_registry_nof_ues(reg) == 0);
  srs_registry_free(reg);
}

static void test_ues_are_kept_apart(void)
{
  srs_registry_t* reg = srs_registry_new();
  SrsSharing__V1__SrsSharingMessage* u;
  feed(reg, config_report(0x4601, 3, CREATED, true, 7, 1), &u);
  drop(u);
  feed(reg, config_report(0x4602, 1, CREATED, true, 9, 2), &u);
  drop(u);
  feed(reg, ta_report(0x4602, RAR, 40, 1000, 3), &u);
  assert(u->foreign_ue_update->ue->rnti == 0x4602 && u->foreign_ue_update->timing_advance->n_ta_steps == 40);
  drop(u);
  assert(srs_registry_nof_ues(reg) == 2);

  srs_packed_t out[8];
  size_t n = srs_registry_snapshot(reg, 7, out, 8);
  assert(n == 2);
  for (size_t i = 0; i < n; ++i)
    free(out[i].buf);
  srs_registry_free(reg);
}

static void test_bad_payloads_are_refused(void)
{
  srs_registry_t* reg = srs_registry_new();
  srs_packed_t out[2];
  uint8_t garbage[] = {0xff, 0xff, 0xff};
  assert(srs_registry_on_payload(reg, SRC_A, garbage, sizeof(garbage), 1, out, 2) == 0);

  /* Another schema version. */
  SrsSharing__V1__ForeignUeUpdate f = SRS_SHARING__V1__FOREIGN_UE_UPDATE__INIT;
  SrsSharing__V1__SrsSharingMessage m = SRS_SHARING__V1__SRS_SHARING_MESSAGE__INIT;
  m.body_case = SRS_SHARING__V1__SRS_SHARING_MESSAGE__BODY_FOREIGN_UE_UPDATE;
  m.foreign_ue_update = &f;
  size_t len;
  uint8_t* buf = pack(&m, 1, 2, &len);
  assert(srs_registry_on_payload(reg, SRC_A, buf, len, 1, out, 2) == 0);
  free(buf);
  /* A ForeignUeUpdate from a dApp: only the xApp sends those. */
  buf = pack(&m, 2, 1, &len);
  assert(srs_registry_on_payload(reg, SRC_A, buf, len, 1, out, 2) == 0);
  free(buf);
  assert(srs_registry_stats(reg).invalid == 3 && srs_registry_stats(reg).messages == 0);
  srs_registry_free(reg);
}

int main(void)
{
  test_config_produces_an_update();
  test_config_without_resource_is_kept_silent();
  test_ledger_rar_then_mac_ce();
  test_ledger_can_go_negative();
  test_mac_ce_without_rar_is_suspect_until_the_next_rar();
  test_sequence_gap_marks_the_reporters_ues();
  test_new_epoch_replaces_and_removal_releases();
  test_ues_are_kept_apart();
  test_bad_payloads_are_refused();
  printf("SRS sharing registry tests passed\n");
  return 0;
}
