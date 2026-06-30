#!/usr/bin/env bash
set -euo pipefail
umask 077

APP_ENV_FILE=${APP_ENV_FILE:-/opt/degen/web.env}
BACKUP_DIR=${BACKUP_DIR:-/opt/degen/backups/db}
LOG_DIR=${LOG_DIR:-/var/log/degen}
RCLONE_CONFIG=${RCLONE_CONFIG:-/etc/degen/rclone.conf}
RCLONE_REMOTE_PATH=${RCLONE_REMOTE_PATH:-onedrive:backups/degen-db}
KEEP_LOCAL_COUNT=${KEEP_LOCAL_COUNT:-2}
KEEP_REMOTE_DAILY=${KEEP_REMOTE_DAILY:-7}
KEEP_REMOTE_WEEKLY=${KEEP_REMOTE_WEEKLY:-4}
KEEP_REMOTE_MONTHLY=${KEEP_REMOTE_MONTHLY:-3}
REMOTE_PRUNE_ENABLED=${REMOTE_PRUNE_ENABLED:-0}
MIN_FREE_AFTER_BYTES=${MIN_FREE_AFTER_BYTES:-10737418240}
RETENTION_PLANNER=${RETENTION_PLANNER:-/usr/local/sbin/degen-prod-db-retention}
LOCK_FILE=${LOCK_FILE:-/run/lock/degen-prod-db-backup.lock}
LOG_FILE=${LOG_FILE:-$LOG_DIR/prod-db-backup.log}

database_url=""
backup_prefix=""
owned_local_dump_partial=""
owned_local_sidecar_partial=""
owned_upload_token_file=""
owned_remote_dump_temp=""
owned_remote_sidecar_temp=""
owns_remote_dump_temp=0
owns_remote_sidecar_temp=0
logger_pid=""
logger_write_fd=""
logger_read_fd=""
original_stdout_fd=""
original_stderr_fd=""


die() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}


log() {
    printf '%s\n' "$*"
}


timestamp_stream() {
    local line timestamp
    while IFS= read -r line || [[ -n "$line" ]]; do
        timestamp=$(date -u '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || printf 'timestamp-unavailable')
        printf '[%s] %s\n' "$timestamp" "$line"
    done
}


start_logging() {
    exec {original_stdout_fd}>&1 {original_stderr_fd}>&2
    coproc LOGGER_PROCESS {
        set -o pipefail
        timestamp_stream | tee -a "$LOG_FILE" >&${original_stdout_fd}
    }
    logger_pid=$LOGGER_PROCESS_PID
    logger_read_fd=${LOGGER_PROCESS[0]}
    logger_write_fd=${LOGGER_PROCESS[1]}
    exec {logger_read_fd}<&-
    exec 1>&${logger_write_fd} 2>&1
    exec {logger_write_fd}>&-
}


warn_cleanup_failure() {
    local category=$1
    local path=$2
    local timestamp basename
    timestamp=$(date -u '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || printf 'timestamp-unavailable')
    basename=${path##*/}
    if [[ "$original_stderr_fd" =~ ^[0-9]+$ ]]; then
        { printf '[%s] WARNING: backup cleanup failed (%s): %s\n' \
            "$timestamp" "$category" "$basename" >&${original_stderr_fd}; } 2>/dev/null || true
    else
        { printf '[%s] WARNING: backup cleanup failed (%s): %s\n' \
            "$timestamp" "$category" "$basename" >&2; } 2>/dev/null || true
    fi
}


cleanup_on_exit() {
    local status=$?
    local logger_status=0
    trap - EXIT HUP INT TERM
    set +e
    if (( status != 0 )); then
        if [[ -n "$owned_local_dump_partial" ]]; then
            if ! rm -f -- "$owned_local_dump_partial" >/dev/null 2>&1; then
                warn_cleanup_failure "local dump partial" "$owned_local_dump_partial"
            fi
        fi
        if [[ -n "$owned_local_sidecar_partial" ]]; then
            if ! rm -f -- "$owned_local_sidecar_partial" >/dev/null 2>&1; then
                warn_cleanup_failure "local checksum partial" "$owned_local_sidecar_partial"
            fi
        fi
        if [[ -n "$owned_upload_token_file" ]]; then
            rm -f -- "$owned_upload_token_file" >/dev/null 2>&1 || true
        fi
        if (( owns_remote_dump_temp == 1 )) && [[ -n "$owned_remote_dump_temp" ]]; then
            if ! rclone --config "$RCLONE_CONFIG" deletefile "$owned_remote_dump_temp" >/dev/null 2>&1; then
                warn_cleanup_failure "remote dump temp" "$owned_remote_dump_temp"
            fi
        fi
        if (( owns_remote_sidecar_temp == 1 )) && [[ -n "$owned_remote_sidecar_temp" ]]; then
            if ! rclone --config "$RCLONE_CONFIG" deletefile "$owned_remote_sidecar_temp" >/dev/null 2>&1; then
                warn_cleanup_failure "remote checksum temp" "$owned_remote_sidecar_temp"
            fi
        fi
    fi
    if [[ -n "$original_stdout_fd" && -n "$original_stderr_fd" ]]; then
        exec 1>&${original_stdout_fd} 2>&${original_stderr_fd}
    fi
    if [[ -n "$logger_pid" ]]; then
        wait "$logger_pid"
        logger_status=$?
    fi
    if (( status == 0 && logger_status != 0 )); then
        status=$logger_status
    fi
    if [[ -n "$original_stdout_fd" ]]; then
        exec {original_stdout_fd}>&-
    fi
    if [[ -n "$original_stderr_fd" ]]; then
        exec {original_stderr_fd}>&-
    fi
    exit "$status"
}


validate_nonnegative_integer() {
    local name=$1
    local value=$2
    [[ "$value" =~ ^[0-9]+$ ]] || die "Invalid configuration: $name must be a nonnegative integer"
}


validate_configuration() {
    validate_nonnegative_integer KEEP_LOCAL_COUNT "$KEEP_LOCAL_COUNT"
    validate_nonnegative_integer KEEP_REMOTE_DAILY "$KEEP_REMOTE_DAILY"
    validate_nonnegative_integer KEEP_REMOTE_WEEKLY "$KEEP_REMOTE_WEEKLY"
    validate_nonnegative_integer KEEP_REMOTE_MONTHLY "$KEEP_REMOTE_MONTHLY"
    validate_nonnegative_integer MIN_FREE_AFTER_BYTES "$MIN_FREE_AFTER_BYTES"
    if [[ "$REMOTE_PRUNE_ENABLED" != "0" && "$REMOTE_PRUNE_ENABLED" != "1" ]]; then
        die "Invalid configuration: REMOTE_PRUNE_ENABLED must be exactly 0 or 1"
    fi
}


require_tools() {
    local tool
    local -a tools=(
        python3 psql pg_dump pg_restore sha256sum stat df awk flock tee rclone
        hostname date mktemp mv rm
    )
    for tool in "${tools[@]}"; do
        command -v "$tool" >/dev/null 2>&1 || die "Required tool is unavailable: $tool"
    done
    [[ -x "$RETENTION_PLANNER" ]] || die "Retention planner is not executable: $RETENTION_PLANNER"
    [[ -r "$RCLONE_CONFIG" ]] || die "Rclone config is not readable"
}


load_database_url() {
    local line value first last
    if [[ -n "${DATABASE_URL:-}" ]]; then
        database_url=$DATABASE_URL
    else
        [[ -r "$APP_ENV_FILE" ]] || die "DATABASE_URL is unset and app environment file is unreadable"
        while IFS= read -r line || [[ -n "$line" ]]; do
            [[ "$line" == DATABASE_URL=* ]] || continue
            value=${line#DATABASE_URL=}
            value=${value%$'\r'}
            if (( ${#value} >= 2 )); then
                first=${value:0:1}
                last=${value: -1}
                if [[ ( "$first" == "'" && "$last" == "'" ) || ( "$first" == '"' && "$last" == '"' ) ]]; then
                    value=${value:1:${#value}-2}
                fi
            fi
            database_url=$value
            break
        done < "$APP_ENV_FILE"
    fi
    unset DATABASE_URL
    [[ -n "$database_url" ]] || die "DATABASE_URL is missing"
    case "$database_url" in
        postgresql+psycopg://*)
            database_url="postgresql://${database_url#postgresql+psycopg://}"
            ;;
        postgresql://*|postgres://*) ;;
        *) die "DATABASE_URL must use a supported PostgreSQL URI scheme" ;;
    esac
}


validate_backup_directory() {
    local owner mode
    [[ ! -L "$BACKUP_DIR" ]] || die "Backup directory must not be a symlink"
    [[ -d "$BACKUP_DIR" ]] || die "Backup directory is not a directory"
    if ! owner=$(stat -c '%u' -- "$BACKUP_DIR" 2>/dev/null); then
        die "Unable to read backup directory ownership"
    fi
    [[ "$owner" == "$EUID" ]] || die "Backup directory must be owned by effective uid $EUID"
    if ! mode=$(stat -c '%a' -- "$BACKUP_DIR" 2>/dev/null); then
        die "Unable to read backup directory permissions"
    fi
    [[ "$mode" =~ ^[0-7]{3,4}$ ]] || die "Backup directory permissions were invalid"
    if (( (8#$mode & 0022) != 0 )); then
        die "Backup directory must not be group or world writable"
    fi
}


validate_label() {
    local label=$1
    [[ -n "$label" && "$label" =~ ^[A-Za-z0-9._-]+$ ]] || die "Unsafe backup label: $label"
}


derive_backup_prefix() {
    local database_name host
    if [[ -n "${BACKUP_PREFIX:-}" ]]; then
        validate_label "$BACKUP_PREFIX"
        backup_prefix=$BACKUP_PREFIX
        return
    fi
    if ! database_name=$(PGDATABASE="$database_url" psql --no-psqlrc --tuples-only --no-align --command 'SELECT current_database();' 2>/dev/null); then
        die "Unable to query the PostgreSQL database name"
    fi
    database_name=${database_name//$'\r'/}
    if ! host=$(hostname -s 2>/dev/null); then
        die "Unable to determine the short hostname"
    fi
    host=${host//$'\r'/}
    validate_label "$database_name"
    validate_label "$host"
    backup_prefix="${database_name}_${host}_"
}


check_remote_access() {
    if ! rclone --config "$RCLONE_CONFIG" lsf "$RCLONE_REMOTE_PATH" --files-only --max-depth 1 >/dev/null 2>&1; then
        die "Rclone remote listing preflight failed"
    fi
}


check_capacity() {
    local database_size free_bytes
    if ! database_size=$(PGDATABASE="$database_url" psql --no-psqlrc --tuples-only --no-align --command 'SELECT pg_database_size(current_database());' 2>/dev/null); then
        die "Unable to query PostgreSQL database size"
    fi
    database_size=${database_size//$'\r'/}
    [[ "$database_size" =~ ^[0-9]+$ ]] || die "PostgreSQL database size was not a nonnegative integer"
    if ! free_bytes=$(df -B1 --output=avail "$BACKUP_DIR" 2>/dev/null | awk 'NR == 2 { gsub(/[[:space:]]/, "", $0); print; exit }'); then
        die "Unable to determine backup filesystem capacity"
    fi
    [[ "$free_bytes" =~ ^[0-9]+$ ]] || die "Backup filesystem capacity was not a nonnegative integer"
    if ! awk -v free="$free_bytes" -v size="$database_size" -v reserve="$MIN_FREE_AFTER_BYTES" \
        'BEGIN { exit !(free >= size + reserve) }'; then
        die "Insufficient backup capacity: free bytes must cover database size plus reserve"
    fi
    log "Capacity preflight passed (database_bytes=$database_size free_bytes=$free_bytes reserve_bytes=$MIN_FREE_AFTER_BYTES)"
}


run_preflight() {
    local mode=$1
    require_tools
    validate_backup_directory
    load_database_url
    derive_backup_prefix
    check_remote_access
    if [[ "$mode" != "remote-retention-dry-run" ]]; then
        check_capacity
    fi
    log "Preflight passed for mode=$mode prefix=$backup_prefix"
}


verify_remote_size() {
    local remote_object=$1
    local expected_size=$2
    local stage=$3
    local response actual_size
    if ! response=$(rclone --config "$RCLONE_CONFIG" lsjson "$remote_object" --stat 2>/dev/null); then
        die "Unable to read $stage remote dump metadata"
    fi
    if ! actual_size=$(printf '%s' "$response" | python3 -c \
        'import json,sys; value=json.load(sys.stdin).get("Size"); print(value if isinstance(value, int) and value >= 0 else "")' \
        2>/dev/null); then
        die "Unable to parse $stage remote dump metadata"
    fi
    [[ "$actual_size" =~ ^[0-9]+$ ]] || die "$stage remote dump size was invalid"
    [[ "$actual_size" == "$expected_size" ]] || die "$stage remote dump size verification failed"
}


verify_remote_sidecar() {
    local remote_object=$1
    local checksum=$2
    local dump_name=$3
    local stage=$4
    if ! rclone --config "$RCLONE_CONFIG" cat "$remote_object" 2>/dev/null | \
        python3 -c \
        'import sys; expected=(sys.argv[1] + "  " + sys.argv[2] + "\n").encode(); raise SystemExit(sys.stdin.buffer.read() != expected)' \
        "$checksum" "$dump_name"; then
        die "$stage remote checksum sidecar verification failed"
    fi
}


load_remote_inventory() {
    local output_name=$1
    local listing
    local -n output_values=$output_name
    output_values=()
    if ! listing=$(rclone --config "$RCLONE_CONFIG" lsf "$RCLONE_REMOTE_PATH" --files-only --max-depth 1 2>/dev/null); then
        die "Unable to inventory remote objects"
    fi
    if [[ -n "$listing" ]]; then
        mapfile -t output_values <<< "$listing"
    fi
}


assert_remote_names_absent() {
    local inventory_name=$1
    local context=$2
    shift 2
    local existing requested existing_folded requested_folded
    local LC_ALL=C
    local -n inventory_values=$inventory_name
    for requested in "$@"; do
        requested_folded=${requested,,}
        for existing in "${inventory_values[@]}"; do
            existing_folded=${existing,,}
            if [[ "$existing_folded" == "$requested_folded" ]]; then
                die "$context remote object collision: $existing"
            fi
        done
    done
}


publish_remote_pair() {
    local dump_path=$1
    local sidecar_path=$2
    local dump_name=$3
    local sidecar_name=$4
    local checksum=$5
    local local_size temp_dump_name temp_sidecar_name final_dump final_sidecar
    local upload_token
    local -a remote_inventory=()

    if ! local_size=$(stat -c '%s' -- "$dump_path" 2>/dev/null); then
        die "Unable to read local dump size"
    fi
    [[ "$local_size" =~ ^[0-9]+$ ]] || die "Local dump size was invalid"

    if ! owned_upload_token_file=$(mktemp "$BACKUP_DIR/.degen-upload-token.XXXXXXXX"); then
        die "Unable to allocate a unique remote upload token"
    fi
    upload_token=${owned_upload_token_file##*.}
    if ! rm -f -- "$owned_upload_token_file"; then
        die "Unable to release the remote upload token file"
    fi
    owned_upload_token_file=""
    [[ "$upload_token" =~ ^[A-Za-z0-9]+$ ]] || die "Remote upload token was unsafe"
    temp_dump_name=".degen-upload-${upload_token}-${dump_name}"
    temp_sidecar_name=".degen-upload-${upload_token}-${sidecar_name}"
    owned_remote_dump_temp="${RCLONE_REMOTE_PATH%/}/$temp_dump_name"
    owned_remote_sidecar_temp="${RCLONE_REMOTE_PATH%/}/$temp_sidecar_name"
    final_dump="${RCLONE_REMOTE_PATH%/}/$dump_name"
    final_sidecar="${RCLONE_REMOTE_PATH%/}/$sidecar_name"

    load_remote_inventory remote_inventory
    assert_remote_names_absent remote_inventory "Pre-upload" \
        "$temp_dump_name" "$temp_sidecar_name" "$dump_name" "$sidecar_name"

    if ! rclone --config "$RCLONE_CONFIG" --immutable copyto "$dump_path" "$owned_remote_dump_temp" >/dev/null 2>&1; then
        log "Remote dump temp was not cleanup-owned after failed upload and remains protected: $temp_dump_name"
        die "Remote dump temporary upload failed"
    fi
    # Accepted caveat: immutable success cannot distinguish a raced identical-byte object.
    owns_remote_dump_temp=1
    if ! rclone --config "$RCLONE_CONFIG" --immutable copyto "$sidecar_path" "$owned_remote_sidecar_temp" >/dev/null 2>&1; then
        log "Remote checksum temp was not cleanup-owned after failed upload and remains protected: $temp_sidecar_name"
        die "Remote checksum temporary upload failed"
    fi
    # Accepted caveat: immutable success cannot distinguish a raced identical-byte object.
    owns_remote_sidecar_temp=1

    verify_remote_size "$owned_remote_dump_temp" "$local_size" "Temporary"
    verify_remote_sidecar "$owned_remote_sidecar_temp" "$checksum" "$dump_name" "Temporary"

    load_remote_inventory remote_inventory
    assert_remote_names_absent remote_inventory "Pre-publish" "$dump_name" "$sidecar_name"

    if ! rclone --config "$RCLONE_CONFIG" --immutable moveto "$owned_remote_dump_temp" "$final_dump" >/dev/null 2>&1; then
        die "Remote dump publish move failed"
    fi
    load_remote_inventory remote_inventory
    assert_remote_names_absent remote_inventory "Post-move temp source" "$temp_dump_name"
    owns_remote_dump_temp=0
    if ! rclone --config "$RCLONE_CONFIG" --immutable moveto "$owned_remote_sidecar_temp" "$final_sidecar" >/dev/null 2>&1; then
        die "Remote checksum publish move failed"
    fi
    load_remote_inventory remote_inventory
    assert_remote_names_absent remote_inventory "Post-move temp source" "$temp_sidecar_name"
    owns_remote_sidecar_temp=0

    verify_remote_size "$final_dump" "$local_size" "Final"
    verify_remote_sidecar "$final_sidecar" "$checksum" "$dump_name" "Final"
    log "Remote backup pair verified: $dump_name and $sidecar_name"
}


collect_local_names() {
    local -n destination=$1
    local path
    local restore_dotglob restore_nullglob
    restore_dotglob=$(shopt -p dotglob || true)
    restore_nullglob=$(shopt -p nullglob || true)
    shopt -s dotglob nullglob
    for path in "$BACKUP_DIR"/*; do
        [[ -f "$path" ]] || continue
        destination+=("${path##*/}")
    done
    eval "$restore_dotglob"
    eval "$restore_nullglob"
}


validate_retention_candidates() {
    local now=$1
    local inventory_name=$2
    local candidates_name=$3
    local candidate remainder stamp inventory_item prior counterpart paired
    local -n inventory_values=$inventory_name
    local -n candidate_values=$candidates_name
    local -a seen_candidates=()

    for candidate in "${candidate_values[@]}"; do
        if [[ -z "$candidate" || "$candidate" == */* || ! "$candidate" =~ ^[A-Za-z0-9._-]+$ || "$candidate" != "$backup_prefix"* ]]; then
            die "Unsafe retention candidate: $candidate"
        fi
        remainder=${candidate#"$backup_prefix"}
        if [[ ! "$remainder" =~ ^[0-9]{8}T[0-9]{6}Z\.dump(\.sha256)?$ ]]; then
            die "Unsafe retention candidate: $candidate"
        fi
        stamp=${remainder%%.dump*}
        if [[ "$stamp" == "$now" || "$stamp" > "$now" ]]; then
            die "Unsafe retention candidate: $candidate is current or future dated"
        fi

        paired=0
        for inventory_item in "${inventory_values[@]}"; do
            if [[ "$inventory_item" == "$candidate" ]]; then
                paired=1
                break
            fi
        done
        (( paired == 1 )) || die "Unsafe retention candidate: $candidate was not in the planned inventory"

        for prior in "${seen_candidates[@]}"; do
            [[ "$prior" != "$candidate" ]] || die "Unsafe retention candidate: duplicate $candidate"
        done
        seen_candidates+=("$candidate")
    done

    for candidate in "${candidate_values[@]}"; do
        if [[ "$candidate" == *.sha256 ]]; then
            counterpart=${candidate%.sha256}
        else
            counterpart="${candidate}.sha256"
        fi
        paired=0
        for prior in "${candidate_values[@]}"; do
            if [[ "$prior" == "$counterpart" ]]; then
                paired=1
                break
            fi
        done
        (( paired == 1 )) || die "Unsafe retention candidate: $candidate was not emitted as a complete pair"
    done
}


planner_candidates() {
    local mode=$1
    local now=$2
    local inventory_name=$3
    local output_name=$4
    local -n inventory_ref=$inventory_name
    local -n output_ref=$output_name
    local planner_output
    local -a policy

    if [[ "$mode" == "local" ]]; then
        policy=(--local-count "$KEEP_LOCAL_COUNT")
    else
        policy=(
            --daily "$KEEP_REMOTE_DAILY"
            --weekly "$KEEP_REMOTE_WEEKLY"
            --monthly "$KEEP_REMOTE_MONTHLY"
        )
    fi
    if ! planner_output=$(printf '%s\n' "${inventory_ref[@]}" | "$RETENTION_PLANNER" \
        --mode "$mode" \
        --prefix "$backup_prefix" \
        --now "$now" \
        "${policy[@]}" \
        --format delete-names); then
        die "Retention planner failed for mode=$mode"
    fi
    if [[ -n "$planner_output" ]]; then
        mapfile -t output_ref <<< "$planner_output"
    fi
    validate_retention_candidates "$now" "$inventory_name" "$output_name"
}


run_local_retention() {
    local now=$1
    local candidate
    local -a inventory=() candidates=()
    collect_local_names inventory
    planner_candidates local "$now" inventory candidates
    if (( ${#candidates[@]} == 0 )); then
        log "Local retention candidates: none"
        return
    fi
    for candidate in "${candidates[@]}"; do
        log "Local retention candidate: $candidate"
    done
    for candidate in "${candidates[@]}"; do
        rm -f -- "$BACKUP_DIR/$candidate"
    done
}


run_remote_retention() {
    local now=$1
    local allow_delete=$2
    local candidate listing
    local -a inventory=() candidates=()
    if ! listing=$(rclone --config "$RCLONE_CONFIG" lsf "$RCLONE_REMOTE_PATH" --files-only --max-depth 1 2>/dev/null); then
        die "Unable to inventory remote backups for retention"
    fi
    if [[ -n "$listing" ]]; then
        mapfile -t inventory <<< "$listing"
    fi
    planner_candidates remote "$now" inventory candidates
    if (( ${#candidates[@]} == 0 )); then
        log "Remote retention candidates: none"
        return
    fi
    for candidate in "${candidates[@]}"; do
        log "Remote retention candidate: $candidate"
    done
    if [[ "$allow_delete" != "1" ]]; then
        for candidate in "${candidates[@]}"; do
            log "Remote retention dry run: would delete $candidate"
        done
        return
    fi
    for candidate in "${candidates[@]}"; do
        if ! rclone --config "$RCLONE_CONFIG" deletefile "${RCLONE_REMOTE_PATH%/}/$candidate" >/dev/null 2>&1; then
            die "Remote retention delete failed: $candidate"
        fi
    done
}


publish_local_no_clobber() {
    local source=$1
    local destination=$2
    local label=$3
    if [[ -e "$destination" || -L "$destination" ]]; then
        die "$label final path already exists"
    fi
    if ! mv -n -T -- "$source" "$destination"; then
        die "$label local publish move failed"
    fi
    if [[ -e "$source" || -L "$source" ]]; then
        die "$label local publish collision detected"
    fi
    [[ -f "$destination" && ! -L "$destination" ]] || die "$label local final was not a regular file"
}


create_backup() {
    local now=$1
    local dump_name sidecar_name dump_path sidecar_path checksum
    dump_name="${backup_prefix}${now}.dump"
    sidecar_name="${dump_name}.sha256"
    dump_path="$BACKUP_DIR/$dump_name"
    sidecar_path="$BACKUP_DIR/$sidecar_name"

    if [[ -e "$dump_path" || -L "$dump_path" || -e "$sidecar_path" || -L "$sidecar_path" ]]; then
        die "Backup final path already exists for timestamp $now"
    fi
    if ! owned_local_dump_partial=$(mktemp "$BACKUP_DIR/.$dump_name.partial.XXXXXXXX"); then
        die "Unable to allocate dump partial"
    fi
    if [[ "$owned_local_dump_partial" != "$BACKUP_DIR/"* || ! -f "$owned_local_dump_partial" || -L "$owned_local_dump_partial" ]]; then
        die "Dump partial allocation was unsafe"
    fi
    if ! owned_local_sidecar_partial=$(mktemp "$BACKUP_DIR/.$sidecar_name.partial.XXXXXXXX"); then
        die "Unable to allocate checksum partial"
    fi
    if [[ "$owned_local_sidecar_partial" != "$BACKUP_DIR/"* || ! -f "$owned_local_sidecar_partial" || -L "$owned_local_sidecar_partial" ]]; then
        die "Checksum partial allocation was unsafe"
    fi

    log "Creating PostgreSQL custom-format backup: $dump_name"
    if ! PGDATABASE="$database_url" pg_dump \
        --format=custom \
        --compress=6 \
        --no-owner \
        --no-acl \
        --file "$owned_local_dump_partial" \
        >/dev/null 2>&1; then
        die "pg_dump failed"
    fi
    if ! pg_restore --list "$owned_local_dump_partial" >/dev/null 2>&1; then
        die "pg_restore validation failed"
    fi
    if ! checksum=$(sha256sum -- "$owned_local_dump_partial" 2>/dev/null | awk '{print $1; exit}'); then
        die "sha256sum failed"
    fi
    [[ "$checksum" =~ ^[0-9a-fA-F]{64}$ ]] || die "sha256sum returned an invalid digest"
    checksum=${checksum,,}
    printf '%s  %s\n' "$checksum" "$dump_name" > "$owned_local_sidecar_partial"

    publish_local_no_clobber "$owned_local_dump_partial" "$dump_path" "Dump"
    owned_local_dump_partial=""
    publish_local_no_clobber "$owned_local_sidecar_partial" "$sidecar_path" "Checksum"
    owned_local_sidecar_partial=""

    publish_remote_pair "$dump_path" "$sidecar_path" "$dump_name" "$sidecar_name" "$checksum"
}


main() {
    local mode now remote_delete_allowed
    trap cleanup_on_exit EXIT
    trap 'exit 129' HUP
    trap 'exit 130' INT
    trap 'exit 143' TERM
    trap '' PIPE

    if (( $# > 1 )); then
        die "Unsupported mode or extra arguments"
    fi
    if (( $# == 0 )); then
        mode=run
    else
        mode=$1
    fi
    case "$mode" in
        run|preflight|remote-retention-dry-run) ;;
        *) die "Unsupported mode: $mode" ;;
    esac

    validate_configuration
    exec 9>"$LOCK_FILE"
    if ! flock -n 9; then
        die "Backup is already running; lock unavailable: $LOCK_FILE"
    fi

    run_preflight "$mode"
    if [[ "$mode" == "preflight" ]]; then
        log "Preflight completed; no dump or retention was performed"
        return
    fi

    now=$(date -u '+%Y%m%dT%H%M%SZ')
    [[ "$now" =~ ^[0-9]{8}T[0-9]{6}Z$ ]] || die "UTC timestamp was invalid"

    if [[ "$mode" == "remote-retention-dry-run" ]]; then
        run_remote_retention "$now" 0
        log "Remote retention dry run completed; no dump or deletion was performed"
        return
    fi

    create_backup "$now"
    run_local_retention "$now"
    remote_delete_allowed=0
    if [[ "$REMOTE_PRUNE_ENABLED" == "1" ]]; then
        remote_delete_allowed=1
    fi
    run_remote_retention "$now" "$remote_delete_allowed"
    log "Backup completed successfully"
}


mkdir -p -- "$BACKUP_DIR" "$LOG_DIR"
start_logging
main "$@"
