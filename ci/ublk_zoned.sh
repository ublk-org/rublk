#!/bin/bash

RUBLK=$1
BLKTESTS_TOP=$2

modprobe ublk_drv

if `$RUBLK features | grep ZONED > /dev/null 2>&1`; then
		echo "kernel support ublk-zoned"

		$RUBLK del -a
		$RUBLK add zoned --zone-size=4 --size=512 > /dev/null 2>&1
		sleep 2

		if `$RUBLK list | grep zoned > /dev/null`; then
				echo "found ublk zoned"
				echo "TEST_DEVS=(/dev/ublkb0)" > $BLKTESTS_TOP/config
		else
				echo "TEST_DEVS=()" > $BLKTESTS_TOP/config
				echo "not found ublk zoned"
		fi
else
		echo "kernel doesn't support ublkzoned"
		echo "TEST_DEVS=()" > $BLKTESTS_TOP/config
fi
