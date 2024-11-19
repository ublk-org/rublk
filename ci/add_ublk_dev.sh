#!/bin/bash

add_ublk_dev()
{
	local dev_id=$1
	shift 1
	local add_cmd="$@"
	local dev_name="/dev/ublkb${dev_id}"

	if [ -b $dev_name ]; then
			echo "error"
	else
		eval "$add_cmd" > /dev/null 2>&1
		sleep 1
		if [ -b $dev_name ]; then
			echo $dev_name
		else
			echo "error"
		fi
	fi
}

build_ublk_dev_list()
{
		local rublk=$1
		local dev_id=$2
		local backing_file=$3
		local dev_list=""
		local dev_path=""

		dev_path=`add_ublk_dev $dev_id $rublk add null -n $dev_id -q 2 -d 64`
		if [ "$dev_path" != "error" ]; then
				dev_list="$dev_path "$dev_list
				let dev_id++
		fi

		truncate -s 128m $backing_file
		dev_path=`add_ublk_dev $dev_id $rublk add loop -n $dev_id -q 2 -d 64 -f $backing_file`
		if [ "$dev_path" != "error" ]; then
				dev_list="$dev_path "$dev_list
				let dev_id++
		fi

		echo $dev_list
}

build_ublk_zoned_list()
{
		local rublk=$1
		local dev_id=$2
		local dev_list=""
		local dev_path=""

		dev_path=`add_ublk_dev $dev_id $rublk add zoned -n $dev_id --zone-size 4`
		if [ "$dev_path" != "error" ]; then
				dev_list="$dev_path "$dev_list
				echo $dev_list
		else
				exit -1
		fi
}


RUBLK=$1
BLKTESTS_TOP=$2
TMPF=`mktemp`

[ ! -f ${RUBLK} ] && echo "rublk binary doesn't exist"
[ ! -d ${BLKTESTS_TOP} ] && echo "blktests dir doesn't exist"
set -x

modprobe ublk_drv
$RUBLK del -a

#add_ublk_dev 0 $RUBLK add null -n 0 -q 2 -d 64
#exit 0

if ZDEVL=`build_ublk_zoned_list $RUBLK 0`; then
	echo "TEST_DEVS=(${ZDEVL})" > $BLKTESTS_TOP/zoned_config
else
	rm -f $BLKTESTS_TOP/zoned_config
fi

DEVL=`build_ublk_dev_list $RUBLK 1 $TMPF`
echo "TEST_DEVS=(${DEVL})" > $BLKTESTS_TOP/config

unlink $TMPF

[ -f $BLKTESTS_TOP/zoned_config ] && cat $BLKTESTS_TOP/zoned_config
cat $BLKTESTS_TOP/config
