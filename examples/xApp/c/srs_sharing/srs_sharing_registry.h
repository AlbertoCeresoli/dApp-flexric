/*
 * [srs-sharing] Registry of the SRS sharing xApp: the UEs that reporting dApps announce, their timing advance ledger,
 * and the ForeignUeUpdate messages to send to the dApps of the other gNBs.
 *
 * Pure logic, no FlexRIC calls, so it can be tested on its own (test_srs_sharing_registry.c). Payloads are the
 * SrsSharingMessage of src/sm/dapp_sm/e3/service_models/srs_sharing_sm/srs_sharing.proto, packed with protobuf-c.
 *
 * Per UE, keyed by its serving cell (PLMN, gNB ID, PCI) and C-RNTI:
 *  - the latest SrsConfigReport (resource, epoch, uplink context, slot anchor); a report with event REMOVED ends the
 *    UE and produces a ForeignUeUpdate with released = true;
 *  - the timing advance ledger (TS 38.213 4.2; RUN-O-RAN Eqs. 1-2), in steps of 16*64*T_c/2^mu:
 *      RAR:    n_ta = TA_RA            (absolute, restarts the ledger)
 *      MAC CE: n_ta += TA_k - 31       (relative, 31 = no change)
 *    missed_command_suspected is set when the ledger cannot be trusted: a MAC CE without a known RAR (the xApp
 *    started after the UE attached), or a gap in the reporting dApp's message sequence (a report, possibly a TA
 *    command, was lost between the dApp and the xApp). It is cleared by the next RAR. RUN-O-RAN's other check,
 *    the plausibility of consecutive range estimates, needs delay measurements and belongs to the receiving dApp.
 *
 * An update is produced for every message that changes a UE with a known resource: a configuration report
 * (including anchor refreshes) or a timing advance command.
 */
#ifndef SRS_SHARING_REGISTRY_H
#define SRS_SHARING_REGISTRY_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Who sent a payload, from the E2SM-DAPP indication header. */
typedef struct {
  uint8_t plmn[3]; /* node-plmn-id, TS 38.413 PLMN Identity octets */
  uint32_t nb_id;  /* node-nb-id */
  uint32_t dapp_id;
} srs_source_t;

/* One packed SrsSharingMessage (ForeignUeUpdate body); buf is malloc'ed, the caller frees it. */
typedef struct {
  uint8_t* buf;
  size_t len;
} srs_packed_t;

typedef struct {
  uint64_t messages;       /* payloads accepted */
  uint64_t invalid;        /* payloads refused: undecodable, other schema version, no body, unexpected body */
  uint64_t sequence_gaps;  /* gaps seen in reporters' sequences */
  uint64_t updates;        /* ForeignUeUpdates produced */
} srs_registry_stats_t;

typedef struct srs_registry srs_registry_t;

srs_registry_t* srs_registry_new(void);
void srs_registry_free(srs_registry_t* reg);

/* Feeds one payload of E3 RAN function RAN_FUNCTION_SRS_SHARING from a reporting dApp. Writes up to max_out packed
 * ForeignUeUpdates to out and returns how many. now_us is the xApp's host-monotonic time for the message header. */
size_t srs_registry_on_payload(srs_registry_t* reg, srs_source_t src, uint8_t const* data, size_t len,
                               uint64_t now_us, srs_packed_t* out, size_t max_out);

/* Packs a ForeignUeUpdate for every UE with a known resource, e.g. for a receiving dApp that just appeared. */
size_t srs_registry_snapshot(srs_registry_t* reg, uint64_t now_us, srs_packed_t* out, size_t max_out);

/* Ends every UE served by the given gNB (its PLMN-Identity digits, e.g. "00101", and gNB ID, as in CellRef), for a
 * node that left the RIC: its UEs are gone with it, and nobody will report their release. Writes up to max_out packed
 * ForeignUeUpdates with released = true, one per such UE that receivers know (one with a resource), and returns how
 * many; every such UE leaves the registry, also when out is full. */
size_t srs_registry_release_gnb(srs_registry_t* reg, char const* plmn, uint64_t gnb_id, uint64_t now_us,
                                srs_packed_t* out, size_t max_out);

size_t srs_registry_nof_ues(srs_registry_t const* reg);
srs_registry_stats_t srs_registry_stats(srs_registry_t const* reg);

#ifdef __cplusplus
}
#endif

#endif
