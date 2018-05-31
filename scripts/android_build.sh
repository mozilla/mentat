#!/bin/bash
# This will eventually become a complete build script, not just for Android

set -e

declare -A android_targets
android_targets=(
	["x86"]="i686-linux-android"
	["arm"]="armv7-linux-androideabi"
	["arm64"]="aarch64-linux-android"
)

if [ "$#" -eq 0 ]
then
	selected_targets=(x86 arm arm64)
else
	for target_arg in "$@"
	do
		[[ -z "${android_targets[$target_arg]+yes}" ]] && echo "Unrecognized target $target_arg. Supported targets: ${!android_targets[@]}" && exit 1
		selected_targets=("${selected_targets[@]}" $target_arg)
	done
	
fi

echo "Building selected targets: ${selected_targets[@]}."

for target in "${selected_targets[@]}"
do
	echo "Building target $target. Signature: ${android_targets[$target]}"
	cargo build -p mentat_ffi --target ${android_targets[$target]} --release
done

