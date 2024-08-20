// SPDX-License-Identifier: GPL-2.0
#ifndef UBLK_BPF_INTERNAL_H
#define UBLK_BPF_INTERNAL_H

extern const struct ublksrv_io_desc *ublk_bpf_get_iod(const struct ublk_bpf_io *io) __ksym;
extern int ublk_bpf_complete_io(const struct ublk_bpf_io *io, int res) __ksym;
extern int ublk_bpf_get_dev_id(const struct ublk_bpf_io *io) __ksym;
extern int ublk_bpf_get_queue_id(const struct ublk_bpf_io *io) __ksym;
extern int ublk_bpf_get_io_tag(const struct ublk_bpf_io *io) __ksym;
extern void ublk_bpf_io_dec_ref(const struct ublk_bpf_io *io, int ret) __ksym;
extern int ublk_bpf_dettach_and_complete_aio(struct bpf_aio *aio, int ret) __ksym;
extern int ublk_bpf_attach_and_prep_aio(const struct ublk_bpf_io *_io, unsigned off, unsigned bytes, struct bpf_aio *aio) __ksym;
#endif
