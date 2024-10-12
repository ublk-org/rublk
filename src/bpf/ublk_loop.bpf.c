// SPDX-License-Identifier: GPL-2.0

#include "vmlinux.h"
#include <linux/const.h>
#include <linux/errno.h>
#include <linux/falloc.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>

//#define DEBUG
#include "ublk_bpf.h"

/* libbpf v1.4.5 is required for struct_ops to work */

struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__uint(max_entries, 1);
	__type(key, int);
	__type(value, int);
} fd_map SEC(".maps");

static inline void ublk_loop_comp_and_release_aio(struct bpf_aio *aio, int ret)
{
	ublk_bpf_dettach_and_complete_aio(aio, ret);
	bpf_aio_release(aio);
}

SEC("struct_ops/bpf_aio_complete_cb")
void BPF_PROG(ublk_loop_comp_cb, struct bpf_aio *aio)
{
	BPF_DBG("aio result %d, back_file %s pos %llx", aio->ret,
			aio->iocb.ki_filp->f_path.dentry->d_name.name,
			aio->iocb.ki_pos - aio->ret);
	ublk_loop_comp_and_release_aio(aio, aio->ret);
}

SEC(".struct_ops.link")
struct bpf_aio_complete_ops loop_ublk_bpf_aio_ops = {
	.id = -1,
	.bpf_aio_complete_cb = (void *)ublk_loop_comp_cb,
};

static inline int ublk_loop_submit_backing_io(const struct ublk_bpf_io *io,
		const struct ublksrv_io_desc *iod, int backing_fd)
{
	unsigned int op_flags = 0;
	struct bpf_aio *aio;
	int res = -EINVAL;
	int op;

	/* translate ublk opcode into backing file's */
	switch (iod->op_flags & 0xff) {
	case 0 /*UBLK_IO_OP_READ*/:
		op = BPF_AIO_OP_FS_READ;
		break;
	case 1 /*UBLK_IO_OP_WRITE*/:
		op = BPF_AIO_OP_FS_WRITE;
		break;
	case 2 /*UBLK_IO_OP_FLUSH*/:
		op = BPF_AIO_OP_FS_FSYNC;
		break;
	case 3 /*UBLK_IO_OP_DISCARD*/:
		op = BPF_AIO_OP_FS_FALLOCATE;
		op_flags = FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE;
		break;
	case 4 /*UBLK_IO_OP_WRITE_SAME*/:
		op = BPF_AIO_OP_FS_FALLOCATE;
		op_flags = FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE;
		break;
	case 5 /*UBLK_IO_OP_WRITE_ZEROES*/:
		op = BPF_AIO_OP_FS_FALLOCATE;
		op_flags = FALLOC_FL_ZERO_RANGE | FALLOC_FL_KEEP_SIZE;
		break;
	default:
		return -EINVAL;
	}

	res = -ENOMEM;
	aio = bpf_aio_alloc(op, 0);
	if (!aio)
		goto fail;

	/* attach aio into the specified range of this io command */
	res = ublk_bpf_attach_and_prep_aio(io, 0, iod->nr_sectors << 9, aio);
	if (res < 0) {
		bpf_printk("bpf aio attaching failed %d\n", res);
		goto fail;
	}

	/* submit this aio onto the backing file */
	res = bpf_aio_submit(aio, backing_fd, iod->start_sector << 9,
			iod->nr_sectors << 9, op_flags);
	if (res < 0) {
		bpf_printk("aio submit failed %d\n", res);
		ublk_loop_comp_and_release_aio(aio, res);
	}
	return 0;
fail:
	return res;
}

static inline int __ublk_loop_handle_io_cmd(const struct ublk_bpf_io *io)
{
	const struct ublksrv_io_desc *iod;
	int res = -EINVAL;
	int fd_key = 0;
	int *fd;

	iod = ublk_bpf_get_iod(io);
	if (!iod) {
		ublk_bpf_complete_io(io, res);
		return UBLK_BPF_IO_HANDLED;
	}

	BPF_DBG("ublk dev %u qid %u: handle io cmd tag %u %lx-%d",
			ublk_bpf_get_dev_id(io),
			ublk_bpf_get_queue_id(io),
			ublk_bpf_get_io_tag(io),
			iod->start_sector << 9,
			iod->nr_sectors << 9);

	/* retrieve backing file descriptor */
	fd = bpf_map_lookup_elem(&fd_map, &fd_key);
	if (!fd)
		goto exit;

	/* handle this io command by submitting IOs on backing file */
	res = ublk_loop_submit_backing_io(io, iod, *fd);

exit:
	/* io cmd can't be completes until this reference is dropped */
	ublk_bpf_io_dec_ref(io, res);

	return UBLK_BPF_IO_HANDLED;
}

SEC("struct_ops.s/ublk_bpf_queue_io_cmd_daemon")
int BPF_PROG(ublk_loop_handle_io_cmd, struct ublk_bpf_io *io)
{
	return __ublk_loop_handle_io_cmd(io);
}

SEC(".struct_ops.link")
struct ublk_bpf_ops loop_ublk_bpf_ops = {
	.dev_id = -1,
	.queue_io_cmd_daemon = (void *)ublk_loop_handle_io_cmd,
};

char LICENSE[] SEC("license") = "GPL";
