/*
 * [srs-sharing] SRS sharing xApp: relays the UEs one gNB's dApp reports to the receiving dApps of the other gNBs, so
 * that they can receive those UEs' SRS.
 *
 * Over E2SM-DAPP (SM ID 255) on every E2 node that offers it:
 *  - report style 1 (E3 data): payloads on E3 RAN function RAN_FUNCTION_SRS_SHARING (20) are SrsSharingMessages from
 *    dApps: SrsConfigReport and TimingAdvanceIndicationMessage from reporting dApps, ReceiverHello from receiving
 *    dApps; other E3 RAN functions are ignored;
 *  - report style 2 (subscription map): which dApps are connected to each node;
 *  - control style 1: each resulting ForeignUeUpdate goes, as an E3 xApp control on RAN function 20, to every
 *    receiving dApp of every node other than the reporting one.
 * A receiving dApp is one that sent a ReceiverHello: it gets the current state of every foreign UE when it first
 * announces itself, and is forgotten when it leaves its node's dApp map or misses three hellos. The roles may be
 * separate dApps or one; only a dApp's hellos make it a receiver.
 * The registry (srs_sharing_registry.c) keeps the UEs and their timing advance ledger and builds the updates.
 *
 * Output: one timestamped checkpoint line per lifecycle event, in the format of the dApps (dapp/srs_sharing/
 * checkpoints.py), tag [srs-xapp]: start, each node subscribed, each receiver announced or gone, each UE's new
 * configuration, timing advance change and release with how many receivers acknowledged it, and every failed
 * control. Updates that only refresh a UE's slot anchor are counted in an "alive" line every 30 s.
 * SRS_XAPP_VERBOSE=1 prints every update and control (and every message received), SRS_XAPP_QUIET=1 errors only;
 * SRS_XAPP_HOLD_MS sets how long a message waits for an earlier one of the same dApp (default 1000).
 * FlexRIC's own lines ("[xApp]: ...") are printed by its library, not by this file.
 *
 * Threads: the indication callbacks run on FlexRIC's threads and only queue work; the main loop drains the queue,
 * sends the controls (control_sm_xapp_api blocks until the node answers) and every two seconds compares its nodes
 * with the RIC's node list (sync_nodes).
 *
 * E2 nodes that come and go: the RIC of this tree sends every xApp its node list again whenever a node connects or
 * leaves, and the xApp library keeps it (stock FlexRIC lists the nodes only once, when the xApp connects). A gNB that
 * connects later is then subscribed; one that leaves is dropped, with its receivers, and its UEs are released to the
 * other gNBs; one that restarts is dropped and subscribed again. As a fallback (e.g. a stock RIC), a control that the
 * RIC answers "E2 node not connected" still stops the xApp with exit status 3 (EXIT_NODES_CHANGED), and
 * tools/run_xapp.sh --respawn starts it again; a control left without an answer is only counted.
 *
 * Usage: xapp_srs_sharing -c <flexric.conf> -p <SM plugin directory>
 *        (or tools/run_xapp.sh xapp_srs_sharing); Ctrl-C stops it. SRS_XAPP_DURATION=<s> stops it after s seconds.
 */
#include "../../../../src/util/alg_ds/alg/defer.h"
#include "../../../../src/util/time_now_us.h"
#include "../../../../src/xApp/e42_xapp_api.h"
#include "../../../../src/xApp/sync_ui.h" /* SYNC_UI_*: why a control failed */

#include "srs_sharing.pb-c.h"
#include "srs_sharing_registry.h"
#include "srs_sharing_reorder.h"

#include <assert.h>
#include <inttypes.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

static int const DAPP_SM_ID = 255;
static uint32_t const E3_RAN_FUNCTION = SRS_SHARING__V1__RAN_FUNCTION__RAN_FUNCTION_SRS_SHARING;
static uint32_t const STYLE_E3_DATA = 1;
static uint32_t const STYLE_SUBSCRIPTION_MAP = 2;

#define MAX_NODES 16
#define MAX_DAPPS 64
#define MAX_RECEIVERS 64
#define MAX_UPDATES 64
#define MAX_TRACKED_UES 256

static volatile sig_atomic_t g_terminate = 0;

/* Exit status when a node turned out not to be connected any more (EXIT_NODES_CHANGED, see send_control). With this
 * tree's RIC the node list follows node changes (sync_nodes) and this only happens in the moments before the list
 * arrives; with a stock FlexRIC, which gives an xApp the nodes only once, restarting the xApp is what picks up the
 * current nodes: tools/run_xapp.sh --respawn does it on this status. */
#define EXIT_NODES_CHANGED 3
static int g_exit_status = 0;

static void* signal_thread(void* arg)
{
  int sig;
  if (sigwait((sigset_t*)arg, &sig) == 0)
    g_terminate = 1;
  return NULL;
}

static uint64_t monotonic_us(void)
{
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000u + (uint64_t)ts.tv_nsec / 1000u;
}

/* ---- checkpoint output ------------------------------------------------------------------------------------- */

static bool g_verbose = false;
static bool g_quiet = false;
static pthread_mutex_t g_print_mtx = PTHREAD_MUTEX_INITIALIZER; /* callbacks print too */

static void vlog_line(FILE* out, char const* fmt, va_list ap)
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  struct tm tm;
  localtime_r(&tv.tv_sec, &tm);
  pthread_mutex_lock(&g_print_mtx);
  fprintf(out, "%02d:%02d:%02d.%03ld [srs-xapp] ", tm.tm_hour, tm.tm_min, tm.tm_sec, (long)(tv.tv_usec / 1000));
  vfprintf(out, fmt, ap);
  fputc('\n', out);
  fflush(out);
  pthread_mutex_unlock(&g_print_mtx);
}

/* A checkpoint: printed unless quiet. */
static void say(char const* fmt, ...)
{
  if (g_quiet)
    return;
  va_list ap;
  va_start(ap, fmt);
  vlog_line(stdout, fmt, ap);
  va_end(ap);
}

/* Always printed. */
static void error_line(char const* fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  vlog_line(stderr, fmt, ap);
  va_end(ap);
}

/* Counts since the last alive line, and since the start. */
static struct {
  uint64_t messages, refreshes, controls_ok, controls_failed;
} g_window, g_total;

/* ---- E2 node identity -------------------------------------------------------------------------------------
 * Indication headers name their node with TS 38.413 PLMN octets (BCD: MCC2|MCC1, MNC3|MCC3, MNC2|MNC1, MNC3 = F
 * for a 2-digit MNC) and the gNB ID; FlexRIC names nodes with decoded MCC/MNC. Both are turned into this key. */
typedef struct {
  uint16_t mcc;
  uint16_t mnc;
  uint8_t mnc_len; /* 2 or 3 digits: "01" and "001" are different MNCs */
  uint32_t nb_id;
} node_key_t;

static node_key_t key_from_header(uint8_t const plmn[3], uint32_t nb_id)
{
  uint16_t mcc = (plmn[0] & 0x0f) * 100 + (plmn[0] >> 4) * 10 + (plmn[1] & 0x0f);
  uint16_t mnc = (plmn[2] & 0x0f) * 10 + (plmn[2] >> 4);
  uint8_t mnc_len = 2;
  if ((plmn[1] >> 4) != 0x0f) {
    mnc = mnc * 10 + (plmn[1] >> 4);
    mnc_len = 3;
  }
  return (node_key_t){.mcc = mcc, .mnc = mnc, .mnc_len = mnc_len, .nb_id = nb_id};
}

static node_key_t key_from_node(global_e2_node_id_t const* id)
{
  return (node_key_t){.mcc = id->plmn.mcc, .mnc = id->plmn.mnc, .mnc_len = id->plmn.mnc_digit_len,
                      .nb_id = id->nb_id.nb_id};
}

static bool same_node(node_key_t a, node_key_t b)
{
  return a.mcc == b.mcc && a.mnc == b.mnc && a.nb_id == b.nb_id;
}

/* "001-01/411": how nodes are named in the output. */
static char const* node_name(node_key_t k, char* buf, size_t len)
{
  snprintf(buf, len, "%03u-%0*u/%u", k.mcc, k.mnc_len == 3 ? 3 : 2, k.mnc, k.nb_id);
  return buf;
}

/* Whether a UE (its serving cell, as SrsSharingMessages name it: PLMN digits and gNB ID) is served by this node. Both
 * parts are needed: different operators can use the same gNB ID (the two lab gNBs both use OCUDU's default, 411). */
static bool serves(node_key_t key, SrsSharing__V1__CellRef const* cell)
{
  char plmn[16]; /* MCC (3 digits) + MNC (2 or 3); sized for any 16-bit values so nothing is ever cut */
  snprintf(plmn, sizeof(plmn), "%03u%0*u", key.mcc, key.mnc_len == 3 ? 3 : 2, key.mnc);
  return cell != NULL && cell->plmn_identity != NULL && strcmp(cell->plmn_identity, plmn) == 0
         && cell->gnb_id == key.nb_id;
}

/* ---- state shared with the callbacks (all under g_mtx) -------------------------------------------------- */

typedef struct work {
  srs_source_t src;
  uint8_t* data;
  size_t len;
  struct work* next;
} work_t;

typedef struct {
  node_key_t key;
  uint32_t dapps[MAX_DAPPS];
  size_t nof_dapps;
} dapp_map_t;

static pthread_mutex_t g_mtx = PTHREAD_MUTEX_INITIALIZER;
static work_t* g_head = NULL;
static work_t* g_tail = NULL;
static dapp_map_t g_maps[MAX_NODES];
static size_t g_nof_maps = 0;

static dapp_map_t* map_of(node_key_t key)
{
  for (size_t i = 0; i < g_nof_maps; ++i)
    if (same_node(g_maps[i].key, key))
      return &g_maps[i];
  if (g_nof_maps == MAX_NODES)
    return NULL;
  dapp_map_t* m = &g_maps[g_nof_maps++];
  memset(m, 0, sizeof(*m));
  m->key = key;
  return m;
}

/* Whether dApp dapp_id is in node's latest map. Call with g_mtx held. */
static bool in_map(node_key_t node, uint32_t dapp_id)
{
  for (size_t i = 0; i < g_nof_maps; ++i)
    if (same_node(g_maps[i].key, node))
      for (size_t j = 0; j < g_maps[i].nof_dapps; ++j)
        if (g_maps[i].dapps[j] == dapp_id)
          return true;
  return false;
}

/* Report style 1: queue SRS sharing payloads; ignore every other E3 RAN function. */
static void on_e3_data(sm_ag_if_rd_t const* rd)
{
  assert(rd != NULL && rd->type == INDICATION_MSG_AGENT_IF_ANS_V0);
  e2sm_dapp_ind_hdr_t const* hdr = &rd->ind.dapp.ind.hdr;
  e2sm_dapp_ind_msg_t const* msg = &rd->ind.dapp.ind.msg;
  if (hdr->format != FORMAT_1_E2SM_DAPP_IND_HDR || msg->format != FORMAT_1_E2SM_DAPP_IND_MSG)
    return;
  if (hdr->frmt_1.ran_function_id != E3_RAN_FUNCTION || msg->frmt_1.data == NULL || msg->frmt_1.data_size == 0)
    return;

  work_t* w = calloc(1, sizeof(work_t));
  assert(w != NULL);
  memcpy(w->src.plmn, hdr->frmt_1.node_plmn_id, 3);
  w->src.nb_id = hdr->frmt_1.node_nb_id;
  w->src.dapp_id = hdr->frmt_1.dapp_id;
  w->len = msg->frmt_1.data_size;
  w->data = malloc(w->len);
  assert(w->data != NULL);
  memcpy(w->data, msg->frmt_1.data, w->len);

  pthread_mutex_lock(&g_mtx);
  if (g_tail != NULL)
    g_tail->next = w;
  else
    g_head = w;
  g_tail = w;
  pthread_mutex_unlock(&g_mtx);
}

/* Report style 2: replace the node's dApp list (receivers that left are dropped by the main loop). */
static void on_subscription_map(sm_ag_if_rd_t const* rd)
{
  assert(rd != NULL && rd->type == INDICATION_MSG_AGENT_IF_ANS_V0);
  e2sm_dapp_ind_hdr_t const* hdr = &rd->ind.dapp.ind.hdr;
  e2sm_dapp_ind_msg_t const* msg = &rd->ind.dapp.ind.msg;
  if (hdr->format != FORMAT_2_E2SM_DAPP_IND_HDR || msg->format != FORMAT_2_E2SM_DAPP_IND_MSG)
    return;
  node_key_t key = key_from_header(hdr->frmt_2.node_plmn_id, hdr->frmt_2.node_nb_id);
  dapp_e3_subscription_list_t const* list = &msg->frmt_2.dapp_e3_subs;

  pthread_mutex_lock(&g_mtx);
  dapp_map_t* m = map_of(key);
  if (m != NULL) {
    m->nof_dapps = 0;
    for (size_t i = 0; i < list->sz_dapp_e3_subscriptions && m->nof_dapps < MAX_DAPPS; ++i)
      m->dapps[m->nof_dapps++] = list->dapp_e3_subscriptions[i].dapp_id;
  }
  pthread_mutex_unlock(&g_mtx);
}

/* ---- E2 nodes and subscriptions (main thread only) ------------------------------------------------------ */

typedef struct {
  global_e2_node_id_t id;
  node_key_t key;
  sm_ans_xapp_t e3_data;
  sm_ans_xapp_t map;
} node_t;

static node_t g_nodes[MAX_NODES];
static size_t g_nof_nodes = 0;

static bool offers_dapp(e2_node_connected_xapp_t const* n)
{
  for (size_t i = 0; i < n->len_rf; ++i)
    if (n->rf[i].id == DAPP_SM_ID)
      return true;
  return false;
}

static sm_ans_xapp_t subscribe(global_e2_node_id_t const* id, uint32_t style, sm_cb cb)
{
  dapp_sub_data_t sub = {0};
  sub.et.format = FORMAT_1_E2SM_DAPP_EV_TRIGGER_FORMAT;
  sub.action_def = calloc(1, sizeof(e2sm_dapp_action_def_t));
  assert(sub.action_def != NULL);
  sub.action_def->format = FORMAT_1_E2SM_DAPP_ACTION_DEF;
  sub.action_def->ric_style_type = style;
  sm_ans_xapp_t ans = report_sm_xapp_api((global_e2_node_id_t*)id, DAPP_SM_ID, &sub, cb);
  free_dapp_sub_data(&sub);
  return ans;
}

/* Subscribes to both report styles of one node; false (and nothing kept) when either is refused. */
static bool subscribe_node(e2_node_connected_xapp_t const* n)
{
  char name[32];
  node_t* node = &g_nodes[g_nof_nodes];
  node->id = cp_global_e2_node_id(&n->id);
  node->key = key_from_node(&n->id);
  node->e3_data = subscribe(&node->id, STYLE_E3_DATA, on_e3_data);
  node->map = subscribe(&node->id, STYLE_SUBSCRIPTION_MAP, on_subscription_map);
  if (!node->e3_data.success || !node->map.success) {
    error_line("node %s: E2SM-DAPP subscription failed", node_name(node->key, name, sizeof(name)));
    free_global_e2_node_id(&node->id);
    return false;
  }
  g_nof_nodes++;
  say("node %s: subscribed to its dApps' reports and dApp map", node_name(node->key, name, sizeof(name)));
  return true;
}

static node_t* node_of(node_key_t key)
{
  for (size_t i = 0; i < g_nof_nodes; ++i)
    if (same_node(g_nodes[i].key, key))
      return &g_nodes[i];
  return NULL;
}

/* ---- receivers (main thread only) --------------------------------------------------------------------------- */

typedef struct {
  node_key_t node;
  uint32_t dapp_id;
  uint64_t last_hello_us;
  uint32_t interval_s;
} receiver_t;

static receiver_t g_receivers[MAX_RECEIVERS];
static size_t g_nof_receivers = 0;

/* Registers or refreshes a receiver; returns true when it is new. */
static bool on_hello(srs_source_t src, SrsSharing__V1__ReceiverHello const* hello)
{
  node_key_t node = key_from_header(src.plmn, src.nb_id);
  for (size_t i = 0; i < g_nof_receivers; ++i)
    if (same_node(g_receivers[i].node, node) && g_receivers[i].dapp_id == src.dapp_id) {
      g_receivers[i].last_hello_us = monotonic_us();
      g_receivers[i].interval_s = hello->resend_interval_s;
      return false;
    }
  if (g_nof_receivers == MAX_RECEIVERS) {
    error_line("too many receivers: dApp %u ignored", src.dapp_id);
    return false;
  }
  g_receivers[g_nof_receivers++] = (receiver_t){
      .node = node, .dapp_id = src.dapp_id, .last_hello_us = monotonic_us(), .interval_s = hello->resend_interval_s};
  char name[32];
  SrsSharing__V1__CellRef const* c = hello->cell;
  say("receiver dApp %u on node %s announced (cell %s/%" PRIu64 "/%u, hello every %u s)", src.dapp_id,
      node_name(node, name, sizeof(name)), c != NULL && c->plmn_identity != NULL ? c->plmn_identity : "?",
      c != NULL ? c->gnb_id : 0, c != NULL ? c->phys_cell_id : 0, hello->resend_interval_s);
  return true;
}

/* Drops receivers that left their node's dApp map or missed three hellos. */
static void expire_receivers(void)
{
  uint64_t const now = monotonic_us();
  for (size_t i = 0; i < g_nof_receivers;) {
    receiver_t const* r = &g_receivers[i];
    pthread_mutex_lock(&g_mtx);
    bool const present = in_map(r->node, r->dapp_id);
    pthread_mutex_unlock(&g_mtx);
    uint64_t const limit = 3ull * (r->interval_s > 0 ? r->interval_s : 30) * 1000000ull;
    bool const silent = now - r->last_hello_us > limit;
    /* A receiver may announce itself before the next map lists it: it only counts as gone when it is still
     * missing from the map a few seconds after its last hello. */
    bool const gone = !present && now - r->last_hello_us > 5000000;
    if (gone || silent) {
      char name[32];
      say("receiver dApp %u on node %s %s", r->dapp_id, node_name(r->node, name, sizeof(name)),
          silent ? "stopped sending hellos: forgotten" : "left its gNB: forgotten");
      g_receivers[i] = g_receivers[--g_nof_receivers];
      continue;
    }
    ++i;
  }
}

static void send_update(srs_packed_t const* p, node_key_t source, long only);

/* A node that left the RIC (the RIC's node list no longer has it): its subscriptions ended with its connection, so
 * they are only forgotten here (a delete would go to a node that is not there, and wait for an answer that never
 * comes). Its receivers go with it, and its UEs are released to the receivers of the other gNBs, since its reporter
 * cannot report their release any more. */
static void drop_node(size_t idx, srs_registry_t* reg)
{
  node_t* node = &g_nodes[idx];
  node_key_t const key = node->key;
  char name[32];
  say("node %s left the RIC: forgetting its subscriptions", node_name(key, name, sizeof(name)));
  for (size_t i = 0; i < g_nof_receivers;) {
    if (same_node(g_receivers[i].node, key)) {
      say("receiver dApp %u on node %s left with its gNB: forgotten", g_receivers[i].dapp_id, name);
      g_receivers[i] = g_receivers[--g_nof_receivers];
      continue;
    }
    ++i;
  }
  pthread_mutex_lock(&g_mtx);
  dapp_map_t* m = map_of(key);
  if (m != NULL)
    m->nof_dapps = 0;
  pthread_mutex_unlock(&g_mtx);
  char plmn[16]; /* as serves() names a cell's PLMN */
  snprintf(plmn, sizeof(plmn), "%03u%0*u", key.mcc, key.mnc_len == 3 ? 3 : 2, key.mnc);
  srs_packed_t updates[MAX_UPDATES];
  size_t const n = srs_registry_release_gnb(reg, plmn, key.nb_id, monotonic_us(), updates, MAX_UPDATES);
  for (size_t i = 0; i < n; ++i) {
    send_update(&updates[i], key, -1);
    free(updates[i].buf);
  }
  free_global_e2_node_id(&node->id);
  g_nodes[idx] = g_nodes[--g_nof_nodes];
}

/* Follows the RIC's E2 node list (a RIC from this tree sends it on every node change; the xApp library keeps it):
 * nodes that left are dropped, new ones subscribed. A node that restarted leaves the list while it is down (a gNB
 * restart takes well over one scan) and comes back without the xApp's subscriptions, so it is subscribed again. A new
 * node is subscribed only once listed in two scans in a row, so that its E2 setup has completed; the nodes listed
 * at the xApp's start are subscribed at once. */
static void sync_nodes(srs_registry_t* reg)
{
  static node_key_t pending[MAX_NODES]; /* listed in the previous scan, not subscribed yet */
  static size_t nof_pending = 0;
  static bool first = true;

  e2_node_arr_xapp_t arr = e2_nodes_xapp_api();
  defer({ free_e2_node_arr_xapp(&arr); });

  for (size_t j = 0; j < g_nof_nodes;) {
    bool listed = false;
    for (size_t i = 0; i < arr.len && !listed; ++i)
      listed = same_node(g_nodes[j].key, key_from_node(&arr.n[i].id));
    if (!listed) {
      drop_node(j, reg);
      continue;
    }
    ++j;
  }

  node_key_t next_pending[MAX_NODES];
  size_t nof_next = 0;
  for (size_t i = 0; i < arr.len; ++i) {
    e2_node_connected_xapp_t const* n = &arr.n[i];
    node_key_t const key = key_from_node(&n->id);
    if (node_of(key) != NULL || !offers_dapp(n) || g_nof_nodes == MAX_NODES)
      continue;
    bool seen = first;
    for (size_t k = 0; k < nof_pending && !seen; ++k)
      seen = same_node(pending[k], key);
    if (!seen || !subscribe_node(n)) {
      if (nof_next < MAX_NODES)
        next_pending[nof_next++] = key;
    }
  }
  memcpy(pending, next_pending, nof_next * sizeof(node_key_t));
  nof_pending = nof_next;
  first = false;
}

/* ---- per-UE view, to tell lifecycle events from routine refreshes ------------------------------------------ */

typedef struct {
  char plmn[8];
  uint64_t gnb_id;
  uint32_t pci, rnti;
  uint64_t epoch;
  bool has_ta, suspect, anchored;
  int64_t n_ta;
} ue_view_t;

static ue_view_t g_ues[MAX_TRACKED_UES];
static size_t g_nof_ues = 0;

typedef enum { UE_NEW, UE_NEW_EPOCH, UE_TA, UE_FIRST_ANCHOR, UE_REFRESH, UE_RELEASED } ue_event_t;

static char const* event_text(ue_event_t e)
{
  switch (e) {
    case UE_NEW:
      return "new";
    case UE_NEW_EPOCH:
      return "new configuration";
    case UE_TA:
      return "timing advance";
    case UE_FIRST_ANCHOR:
      return "first slot anchor";
    case UE_REFRESH:
      return "anchor refresh";
    case UE_RELEASED:
      return "released";
  }
  return "?";
}

/* Compares an update with the previous one for the same UE. */
static ue_event_t classify(SrsSharing__V1__ForeignUeUpdate const* f)
{
  SrsSharing__V1__CellRef const* c = f->ue->serving_cell;
  ue_view_t* v = NULL;
  for (size_t i = 0; i < g_nof_ues && v == NULL; ++i)
    if (strcmp(g_ues[i].plmn, c->plmn_identity) == 0 && g_ues[i].gnb_id == c->gnb_id
        && g_ues[i].pci == c->phys_cell_id && g_ues[i].rnti == f->ue->rnti)
      v = &g_ues[i];
  if (f->released) {
    if (v != NULL)
      *v = g_ues[--g_nof_ues];
    return UE_RELEASED;
  }
  ue_view_t now = {.gnb_id = c->gnb_id, .pci = c->phys_cell_id, .rnti = f->ue->rnti, .epoch = f->config_epoch,
                   .has_ta = f->timing_advance != NULL, .anchored = f->anchor != NULL};
  strncpy(now.plmn, c->plmn_identity, sizeof(now.plmn) - 1);
  if (f->timing_advance != NULL) {
    now.n_ta = f->timing_advance->n_ta_steps;
    now.suspect = f->timing_advance->missed_command_suspected;
  }
  ue_event_t e;
  if (v == NULL) {
    e = UE_NEW;
    if (g_nof_ues < MAX_TRACKED_UES)
      v = &g_ues[g_nof_ues++];
  } else if (v->epoch != now.epoch) {
    e = UE_NEW_EPOCH;
  } else if (v->has_ta != now.has_ta || v->n_ta != now.n_ta || v->suspect != now.suspect) {
    e = UE_TA;
  } else if (!v->anchored && now.anchored) {
    e = UE_FIRST_ANCHOR;
  } else {
    e = UE_REFRESH;
  }
  if (v != NULL)
    *v = now;
  return e;
}

/* "UE 00101/411/1 0x4601 epoch 2 new: period 20 offset 7, anchor 227.7, N_TA 17 steps (1 commands)" */
static void ue_text(SrsSharing__V1__ForeignUeUpdate const* f, ue_event_t e, char* buf, size_t len)
{
  SrsSharing__V1__CellRef const* c = f->ue->serving_cell;
  int n = snprintf(buf, len, "UE %s/%" PRIu64 "/%u 0x%04x epoch %" PRIu64 " %s", c->plmn_identity, c->gnb_id,
                   c->phys_cell_id, f->ue->rnti, f->config_epoch, event_text(e));
  if (e == UE_RELEASED || n < 0 || (size_t)n >= len)
    return;
  if ((e == UE_NEW || e == UE_NEW_EPOCH) && f->resource != NULL && f->resource->periodicity_and_offset != NULL)
    n += snprintf(buf + n, len - n, ": period %u offset %u", f->resource->periodicity_and_offset->period,
                  f->resource->periodicity_and_offset->offset);
  if (f->anchor != NULL && (size_t)n < len)
    n += snprintf(buf + n, len - n, ", anchor %u.%u", f->anchor->sfn, f->anchor->slot);
  if (f->timing_advance != NULL && (size_t)n < len)
    snprintf(buf + n, len - n, ", N_TA %" PRId64 " steps (%u commands%s)", f->timing_advance->n_ta_steps,
             f->timing_advance->nof_commands,
             f->timing_advance->missed_command_suspected ? ", missed command suspected" : "");
}

/* ---- sending ----------------------------------------------------------------------------------------- */

static bool send_control(node_t* node, uint32_t dapp_id, srs_packed_t const* p)
{
  dapp_ctrl_req_data_t ctrl = {0};
  ctrl.hdr.format = FORMAT_1_E2SM_DAPP_CTRL_HDR;
  ctrl.hdr.frmt_1.ran_function_id = E3_RAN_FUNCTION;
  ctrl.hdr.frmt_1.dapp_id = dapp_id;
  ctrl.msg.format = FORMAT_1_E2SM_DAPP_CTRL_MSG;
  /* Raw payload, e3 NONE: the SM plugin encodes it as given. Not freed here: p owns the bytes. */
  ctrl.msg.frmt_1.data = p->buf;
  ctrl.msg.frmt_1.data_size = (uint32_t)p->len;
  ctrl.e3.type = DAPP_E3_SM_NONE;
  sm_ans_xapp_t ans = control_sm_xapp_api(&node->id, DAPP_SM_ID, &ctrl);
  char name[32];
  if (ans.success) {
    g_window.controls_ok++;
    g_total.controls_ok++;
    if (g_verbose)
      say("  -> node %s dApp %u: ack", node_name(node->key, name, sizeof(name)), dapp_id);
  } else {
    g_window.controls_failed++;
    g_total.controls_failed++;
    char const* reason = ans.u.reason != NULL ? ans.u.reason : "failed";
    error_line("control to dApp %u on node %s: %s", dapp_id, node_name(node->key, name, sizeof(name)), reason);
    /* The RIC says the node is not connected: with a stock FlexRIC the xApp would never learn when it comes back
     * (sync_nodes needs this tree's RIC), so stop and let a restart subscribe to the nodes connected now. A control
     * left without an answer, or refused by the node (e.g. a dApp that just left), is only counted: the next anchor
     * refresh, within seconds, brings the receiver up to date, and a node that really left is dropped by sync_nodes. */
    if (strcmp(reason, SYNC_UI_NODE_NOT_CONNECTED) == 0) {
      error_line("node %s is no longer connected: stopping (exit status %d) so that a restart picks up the current "
                 "E2 nodes", node_name(node->key, name, sizeof(name)), EXIT_NODES_CHANGED);
      g_exit_status = EXIT_NODES_CHANGED;
      g_terminate = 1;
    }
  }
  return ans.success;
}

/* Sends one update to every receiver outside the source node, or only to receiver `only` when it is >= 0 (snapshot),
 * and prints its checkpoint. */
static void send_update(srs_packed_t const* p, node_key_t source, long only)
{
  SrsSharing__V1__SrsSharingMessage* m = srs_sharing__v1__srs_sharing_message__unpack(NULL, p->len, p->buf);
  if (m == NULL || m->foreign_ue_update == NULL) {
    if (m != NULL)
      srs_sharing__v1__srs_sharing_message__free_unpacked(m, NULL);
    return;
  }
  ue_event_t e = only >= 0 ? UE_NEW : classify(m->foreign_ue_update);
  size_t targets = 0, acked = 0;
  for (size_t i = 0; i < g_nof_receivers; ++i) {
    if (only >= 0 ? (size_t)only != i : same_node(g_receivers[i].node, source))
      continue;
    node_t* node = node_of(g_receivers[i].node);
    if (node == NULL)
      continue;
    targets++;
    acked += send_control(node, g_receivers[i].dapp_id, p);
  }
  char text[256];
  ue_text(m->foreign_ue_update, e, text, sizeof(text));
  if (e == UE_REFRESH && !g_verbose) {
    g_window.refreshes++;
    g_total.refreshes++;
  } else if (targets == 0) {
    say("%s -> no receiver on another gNB yet", text);
  } else {
    say("%s%s -> %zu/%zu receiver(s) acked", text, only >= 0 ? " (snapshot)" : "", acked, targets);
  }
  srs_sharing__v1__srs_sharing_message__free_unpacked(m, NULL);
}

/* A new receiver gets every known UE not served by its own node. */
static void send_snapshot(srs_registry_t* reg, size_t receiver)
{
  srs_packed_t updates[MAX_UPDATES];
  size_t n = srs_registry_snapshot(reg, monotonic_us(), updates, MAX_UPDATES);
  size_t sent = 0;
  for (size_t i = 0; i < n; ++i) {
    SrsSharing__V1__SrsSharingMessage* m = srs_sharing__v1__srs_sharing_message__unpack(NULL, updates[i].len,
                                                                                       updates[i].buf);
    bool const own = m != NULL && serves(g_receivers[receiver].node, m->foreign_ue_update->ue->serving_cell);
    if (m != NULL)
      srs_sharing__v1__srs_sharing_message__free_unpacked(m, NULL);
    if (!own) {
      send_update(&updates[i], (node_key_t){0}, (long)receiver);
      sent++;
    }
    free(updates[i].buf);
  }
  if (sent == 0)
    say("  no foreign UE known yet for it");
}

/* Handles one message, in its sender's sequence order. */
static void handle(srs_registry_t* reg, srs_source_t src, uint8_t const* data, size_t len)
{
  g_window.messages++;
  g_total.messages++;
  node_key_t const source = key_from_header(src.plmn, src.nb_id);
  /* Hellos are about receivers, not UEs: the xApp keeps them; everything else goes to the registry. */
  SrsSharing__V1__SrsSharingMessage* m = srs_sharing__v1__srs_sharing_message__unpack(NULL, len, data);
  if (m != NULL && m->body_case == SRS_SHARING__V1__SRS_SHARING_MESSAGE__BODY_RECEIVER_HELLO) {
    if (on_hello(src, m->receiver_hello))
      send_snapshot(reg, g_nof_receivers - 1);
  } else {
    srs_packed_t updates[MAX_UPDATES];
    size_t n = srs_registry_on_payload(reg, src, data, len, monotonic_us(), updates, MAX_UPDATES);
    for (size_t i = 0; i < n; ++i) {
      send_update(&updates[i], source, -1);
      free(updates[i].buf);
    }
  }
  if (m != NULL)
    srs_sharing__v1__srs_sharing_message__free_unpacked(m, NULL);
}

/* Moves what the callbacks queued into the reorder buffer (the RIC can swap messages a dApp sent milliseconds apart),
 * then handles everything due, in each sender's sequence order. */
static void drain(srs_registry_t* reg, srs_reorder_t* reorder)
{
  pthread_mutex_lock(&g_mtx);
  work_t* w = g_head;
  g_head = g_tail = NULL;
  pthread_mutex_unlock(&g_mtx);
  uint64_t const now = monotonic_us();
  while (w != NULL) {
    SrsSharing__V1__SrsSharingMessage* m = srs_sharing__v1__srs_sharing_message__unpack(NULL, w->len, w->data);
    if (m != NULL && m->header != NULL) {
      if (g_verbose) {
        char name[32];
        say("  <- node %s dApp %u: sequence %" PRIu64 ", body %d",
            node_name(key_from_header(w->src.plmn, w->src.nb_id), name, sizeof(name)), w->src.dapp_id,
            m->header->sequence, (int)m->body_case);
      }
      srs_reorder_push(reorder, (srs_item_t){.src = w->src, .sequence = m->header->sequence, .data = w->data,
                                             .len = w->len}, now);
      w->data = NULL;  /* now owned by the reorder buffer */
    } else {
      handle(reg, w->src, w->data, w->len);  /* undecodable: the registry counts it as refused */
    }
    if (m != NULL)
      srs_sharing__v1__srs_sharing_message__free_unpacked(m, NULL);
    work_t* next = w->next;
    free(w->data);
    free(w);
    w = next;
  }
  srs_item_t item;
  while (srs_reorder_pop(reorder, monotonic_us(), &item)) {
    handle(reg, item.src, item.data, item.len);
    free(item.data);
  }
}

static void discard_queue(void)
{
  pthread_mutex_lock(&g_mtx);
  work_t* w = g_head;
  g_head = g_tail = NULL;
  pthread_mutex_unlock(&g_mtx);
  while (w != NULL) {
    work_t* next = w->next;
    free(w->data);
    free(w);
    w = next;
  }
}

static double env_number(char const* name, double fallback)
{
  char const* v = getenv(name);
  return v != NULL && *v != '\0' ? atof(v) : fallback;
}

int main(int argc, char* argv[])
{
  /* Line-buffered output even into a file: FlexRIC's library prints with printf, and a block-buffered log loses its
   * last lines when the process dies, which are the ones that explain why. */
  setvbuf(stdout, NULL, _IOLBF, 0);
  fr_args_t args = init_fr_args(argc, argv);

  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGINT);
  sigaddset(&set, SIGTERM);
  int rc = pthread_sigmask(SIG_BLOCK, &set, NULL);
  assert(rc == 0);
  pthread_t sig_thr;
  rc = pthread_create(&sig_thr, NULL, signal_thread, &set);
  assert(rc == 0);

  double const duration_s = env_number("SRS_XAPP_DURATION", 0.0);
  double const alive_s = env_number("SRS_XAPP_ALIVE", 30.0);
  g_verbose = env_number("SRS_XAPP_VERBOSE", 0.0) != 0.0;
  g_quiet = env_number("SRS_XAPP_QUIET", 0.0) != 0.0;

  init_xapp_api(&args);
  sleep(1);

  srs_registry_t* reg = srs_registry_new();
  assert(reg != NULL);
  /* Swaps seen live are a few milliseconds, but one straggler came more than 200 ms late; waiting only costs anything
   * when a message is missing, so wait up to a second before taking it as lost. */
  srs_reorder_t* reorder = srs_reorder_new((uint64_t)(env_number("SRS_XAPP_HOLD_MS", 1000.0) * 1000));
  assert(reorder != NULL);
  say("started: relaying E3 RAN function %u from reporting dApps to the receiving dApps of other gNBs",
      E3_RAN_FUNCTION);

  uint64_t const start = monotonic_us();
  uint64_t next_scan = 0;
  uint64_t next_alive = start + (uint64_t)(alive_s * 1e6);
  while (!g_terminate && (duration_s <= 0 || monotonic_us() - start < (uint64_t)(duration_s * 1e6))) {
    if (monotonic_us() >= next_scan) {
      sync_nodes(reg);
      expire_receivers();
      next_scan = monotonic_us() + 2000000;
    }
    drain(reg, reorder);
    if (monotonic_us() >= next_alive) {
      say("alive: %zu node(s), %zu receiver(s), %zu UE(s); %" PRIu64 " messages, %" PRIu64
          " anchor refreshes relayed, controls %" PRIu64 " ack / %" PRIu64 " failed in the last %.0f s",
          g_nof_nodes, g_nof_receivers, srs_registry_nof_ues(reg), g_window.messages, g_window.refreshes,
          g_window.controls_ok, g_window.controls_failed, alive_s);
      memset(&g_window, 0, sizeof(g_window));
      next_alive += (uint64_t)(alive_s * 1e6);
    }
    usleep(10000);
  }

  srs_registry_stats_t s = srs_registry_stats(reg);
  srs_reorder_stats_t o = srs_reorder_stats(reorder);
  say("stopped: %" PRIu64 " messages (%" PRIu64 " refused, %" PRIu64 " sequence gaps; %" PRIu64 " put back in order, %"
      PRIu64 " lost, %" PRIu64 " dropped), %" PRIu64 " updates, controls %" PRIu64 " ack / %" PRIu64 " failed, %zu UE(s)",
      g_total.messages, s.invalid, s.sequence_gaps, o.reordered, o.given_up, o.dropped, s.updates,
      g_total.controls_ok, g_total.controls_failed, srs_registry_nof_ues(reg));

  for (size_t i = 0; i < g_nof_nodes; ++i) {
    rm_report_sm_xapp_api(g_nodes[i].e3_data.u.handle);
    rm_report_sm_xapp_api(g_nodes[i].map.u.handle);
  }
  sleep(1);
  while (try_stop_xapp_api() == false)
    usleep(1000);
  for (size_t i = 0; i < g_nof_nodes; ++i)
    free_global_e2_node_id(&g_nodes[i].id);
  discard_queue(); /* what arrived during shutdown: the xApp API is stopped, nothing can be sent */
  srs_reorder_free(reorder);
  srs_registry_free(reg);
  return g_exit_status;
}
