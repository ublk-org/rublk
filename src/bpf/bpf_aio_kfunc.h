// SPDX-License-Identifier: GPL-2.0
#ifndef UBLK_BPF_AIO_INTERNAL_H
#define UBLK_BPF_AIO_INTERNAL_H

extern struct bpf_aio *bpf_aio_alloc(unsigned int op, enum bpf_aio_flag flags) __ksym;
extern struct bpf_aio *bpf_aio_alloc_sleepable(unsigned int op, enum bpf_aio_flag flags) __ksym;
extern void bpf_aio_release(struct bpf_aio *aio) __ksym;
extern int bpf_aio_submit(struct bpf_aio *aio, int fd, loff_t pos,
                unsigned bytes, unsigned io_flags) __ksym;
#endif
