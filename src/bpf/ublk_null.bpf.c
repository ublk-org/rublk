// SPDX-License-Identifier: GPL-2.0

#include "vmlinux.h"
#include <linux/const.h>
#include <linux/errno.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>

//#define DEBUG
#include "ublk_bpf.h"

/* libbpf v1.4.5 is required for struct_ops to work */

static inline int __ublk_null_handle_io(const struct ublk_bpf_io *io)
{
	unsigned long off = -1, sects = -1;
	const struct ublksrv_io_desc *iod;
	int res;

	iod = ublk_bpf_get_iod(io);
	if (iod) {
		res = iod->nr_sectors << 9;
		off = iod->start_sector;
		sects = iod->nr_sectors;
	} else
		res = -EINVAL;
	BPF_DBG("ublk dev %u qid %u: handle io tag %u %lx-%d res %d",
			ublk_bpf_get_dev_id(io),
			ublk_bpf_get_queue_id(io),
			ublk_bpf_get_io_tag(io),
			off, sects, res);
	ublk_bpf_complete_io(io, res);
	return UBLK_BPF_IO_HANDLED;
}

SEC("struct_ops/ublk_bpf_queue_io_cmd")
int BPF_PROG(ublk_null_handle_io, struct ublk_bpf_io *io)
{
	return __ublk_null_handle_io(io);
}

SEC(".struct_ops.link")
struct ublk_bpf_ops null_ublk_bpf_ops = {
	.dev_id = -1,
	.queue_io_cmd = (void *)ublk_null_handle_io,
};

char LICENSE[] SEC("license") = "GPL";
