// SPDX-License-Identifier: GPL-2.0
#ifndef UBLK_BPF_GEN_H
#define UBLK_BPF_GEN_H

#include "ublk_bpf_kfunc.h"
#include "bpf_aio_kfunc.h"

#ifdef DEBUG
#define BPF_DBG(...) bpf_printk(__VA_ARGS__)
#else
#define BPF_DBG(...)
#endif

#endif
