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

```console
$ rublk help
Usage: rublk <COMMAND>

Commands:
  add       Adds ublk target
  del       Deletes ublk target
  list      Lists ublk targets
  recover   Recover ublk targets
  features  Get supported features from ublk driver, supported since v6.5
  help      Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

rublk add help

```console
$ rublk add help
Adds ublk target

Usage: rublk add <COMMAND>

Commands:
  loop   Add loop target
  null   Add null target
  zoned  Add zoned target, supported since v6.6
  qcow2  Add qcow2 target
  help   Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
```

## License

This project is licensed under GPL-2.0.

## Contributing

Any kinds of contributions are welcome!

[^1]: <https://crates.io/crates/libublk>
[^2]: <https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/drivers/block/ublk_drv.c?h=v6.0>
