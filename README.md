# rublk

[![License: GPL v2](https://img.shields.io/badge/License-GPL_v2-blue.svg)] (https://github.com/ming1/rublk/blob/master/COPYING)

Rust ublk generic target implementation, which depends on `libublk`[^1],
which talks with linux `ublk driver`[^2] for building and exposing standard
linux block device, meantime all target IO logic is moved to userspace.

Linux kernel 6.0 starts to support ublk covered by config option of
CONFIG_BLK_DEV_UBLK.

## Documentations

[ublk doc
links](https://github.com/ming1/ubdsrv/blob/master/doc/external_links.rst)

[ublk
introduction](https://github.com/ming1/ubdsrv/blob/master/doc/ublk_intro.pdf)

## Quick Start

cargo install rublk

modprobe ublk_drv

rublk help

rublk add help

## License

This project is licensed under GPL-2.0.

## Contributing

Any kinds of contributions are welcome!

[^1]: <https://crates.io/crates/libublk>
[^2]: <https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/drivers/block/ublk_drv.c?h=v6.0>
