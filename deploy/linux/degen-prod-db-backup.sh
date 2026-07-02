#!/usr/bin/env bash
set -euo pipefail
umask 077

readonly FIXED_BACKUP_ENV_FILE="/etc/degen/prod-db-backup.env"
readonly FIXED_ENV_HELPER="/usr/local/sbin/degen-prod-db-backup-env"
readonly -a MANAGED_DEFAULT_KEYS=(
    APP_ENV_FILE
    BACKUP_DIR
    LOG_DIR
    RCLONE_CONFIG
    RCLONE_REMOTE_PATH
    KEEP_LOCAL_COUNT
    KEEP_REMOTE_DAILY
    KEEP_REMOTE_WEEKLY
    KEEP_REMOTE_MONTHLY
    REMOTE_PRUNE_ENABLED
    MIN_FREE_AFTER_BYTES
    RETENTION_PLANNER
    LOCK_FILE
)
readonly -a MANAGED_KEYS=("${MANAGED_DEFAULT_KEYS[@]}" BACKUP_PREFIX)

database_url=""
backup_prefix=""
runtime_env_file=""
runtime_env_helper=""
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
rclone_receipt_started=0
rclone_receipt_finished=0
rclone_receipt_before_metadata=""


die() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}


log() {
    printf '%s\n' "$*"
}


resolve_runtime_configuration_paths() {
    local test_variable_present=0
    local variable

    if [[ -v BACKUP_ENV_FILE && "$BACKUP_ENV_FILE" != "$FIXED_BACKUP_ENV_FILE" ]]; then
        die "Invalid backup runtime configuration"
    fi
    if [[ -v ENV_HELPER && "$ENV_HELPER" != "$FIXED_ENV_HELPER" ]]; then
        die "Invalid backup runtime configuration"
    fi

    runtime_env_file=$FIXED_BACKUP_ENV_FILE
    runtime_env_helper=$FIXED_ENV_HELPER
    for variable in \
        DEGEN_BACKUP_TEST_MODE \
        DEGEN_BACKUP_TEST_ENV_FILE \
        DEGEN_BACKUP_TEST_ENV_HELPER \
        DEGEN_BACKUP_TEST_LOGGER_EXIT_AFTER \
        DEGEN_BACKUP_TEST_LOGGER_REPLACE_PATH_AFTER_OPEN; do
        if [[ -v "$variable" ]]; then
            test_variable_present=1
        fi
    done
    if (( test_variable_present == 0 )); then
        return
    fi
    if (( EUID == 0 )); then
        die "Test backup configuration is not permitted"
    fi
    if [[ "${DEGEN_BACKUP_TEST_MODE:-}" != "1" || \
          -z "${DEGEN_BACKUP_TEST_ENV_FILE:-}" || \
          -z "${DEGEN_BACKUP_TEST_ENV_HELPER:-}" ]]; then
        die "Invalid backup runtime configuration"
    fi
    runtime_env_file=$DEGEN_BACKUP_TEST_ENV_FILE
    runtime_env_helper=$DEGEN_BACKUP_TEST_ENV_HELPER
}


managed_key_from_line() {
    local line=$1
    case "$line" in
        APP_ENV_FILE=*) printf 'APP_ENV_FILE\n' ;;
        BACKUP_DIR=*) printf 'BACKUP_DIR\n' ;;
        LOG_DIR=*) printf 'LOG_DIR\n' ;;
        RCLONE_CONFIG=*) printf 'RCLONE_CONFIG\n' ;;
        RCLONE_REMOTE_PATH=*) printf 'RCLONE_REMOTE_PATH\n' ;;
        KEEP_LOCAL_COUNT=*) printf 'KEEP_LOCAL_COUNT\n' ;;
        KEEP_REMOTE_DAILY=*) printf 'KEEP_REMOTE_DAILY\n' ;;
        KEEP_REMOTE_WEEKLY=*) printf 'KEEP_REMOTE_WEEKLY\n' ;;
        KEEP_REMOTE_MONTHLY=*) printf 'KEEP_REMOTE_MONTHLY\n' ;;
        REMOTE_PRUNE_ENABLED=*) printf 'REMOTE_PRUNE_ENABLED\n' ;;
        MIN_FREE_AFTER_BYTES=*) printf 'MIN_FREE_AFTER_BYTES\n' ;;
        RETENTION_PLANNER=*) printf 'RETENTION_PLANNER\n' ;;
        LOCK_FILE=*) printf 'LOCK_FILE\n' ;;
        BACKUP_PREFIX=*) printf 'BACKUP_PREFIX\n' ;;
        *) return 1 ;;
    esac
}


load_managed_configuration() {
    local key line value remaining helper_capture helper_output helper_status
    local inherited_log_file_present=0
    local inherited_log_file=""
    local -A inherited_present=()
    local -A inherited_values=()
    local -A parsed_present=()
    local -A parsed_values=()

    for key in "${MANAGED_KEYS[@]}"; do
        if [[ -v "$key" ]]; then
            inherited_present["$key"]=1
            inherited_values["$key"]=${!key}
        fi
    done
    if [[ -v LOG_FILE ]]; then
        inherited_log_file_present=1
        inherited_log_file=$LOG_FILE
    fi

    helper_capture=$(
        set +e
        "$runtime_env_helper" emit --file "$runtime_env_file" 2>/dev/null
        helper_status=$?
        printf '\037%s' "$helper_status"
        exit 0
    )
    if [[ "$helper_capture" != *$'\037'* ]]; then
        die "Invalid managed backup configuration"
    fi
    helper_status=${helper_capture##*$'\037'}
    helper_output=${helper_capture%$'\037'*}
    if [[ ! "$helper_status" =~ ^[0-9]+$ || "$helper_status" != "0" || \
          -z "$helper_output" || "$helper_output" != *$'\n' ]]; then
        die "Invalid managed backup configuration"
    fi

    remaining=$helper_output
    while [[ -n "$remaining" ]]; do
        if [[ "$remaining" != *$'\n'* ]]; then
            die "Invalid managed backup configuration"
        fi
        line=${remaining%%$'\n'*}
        remaining=${remaining#*$'\n'}
        if [[ -z "$line" || "$line" == *$'\r'* ]]; then
            die "Invalid managed backup configuration"
        fi
        if ! key=$(managed_key_from_line "$line"); then
            die "Invalid managed backup configuration"
        fi
        if [[ -v "parsed_present[$key]" ]]; then
            die "Invalid managed backup configuration"
        fi
        value=${line#*=}
        if [[ -z "$value" || ! "$value" =~ ^[A-Za-z0-9_./:@%+,?=-]+$ ]]; then
            die "Invalid managed backup configuration"
        fi
        parsed_present["$key"]=1
        parsed_values["$key"]=$value
    done

    for key in "${MANAGED_DEFAULT_KEYS[@]}"; do
        if [[ ! -v "parsed_present[$key]" ]]; then
            die "Invalid managed backup configuration"
        fi
    done
    for key in "${MANAGED_KEYS[@]}"; do
        if [[ -v "inherited_present[$key]" ]]; then
            if [[ ! -v "parsed_present[$key]" || \
                  "${inherited_values[$key]}" != "${parsed_values[$key]}" ]]; then
                die "Invalid managed backup configuration"
            fi
        fi
    done

    for key in "${MANAGED_DEFAULT_KEYS[@]}"; do
        printf -v "$key" '%s' "${parsed_values[$key]}"
    done
    if [[ -v 'parsed_present[BACKUP_PREFIX]' ]]; then
        printf -v BACKUP_PREFIX '%s' "${parsed_values[BACKUP_PREFIX]}"
    else
        unset BACKUP_PREFIX
    fi
    LOG_FILE="$LOG_DIR/prod-db-backup.log"
    if (( inherited_log_file_present == 1 )) && [[ "$inherited_log_file" != "$LOG_FILE" ]]; then
        die "Invalid managed backup configuration"
    fi
}


timestamp_stream() {
    local line timestamp
    while IFS= read -r line || [[ -n "$line" ]]; do
        timestamp=$(date -u '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || printf 'timestamp-unavailable')
        printf '[%s] %s\n' "$timestamp" "$line"
    done
}


run_secure_log_helper() {
    local action=$1
    local test_exit_after=${DEGEN_BACKUP_TEST_LOGGER_EXIT_AFTER:-}
    /usr/bin/python3 -c '
import os
import stat
import sys


def fail():
    raise RuntimeError("secure logger validation failed")


def same_identity(left, right):
    return left.st_dev == right.st_dev and left.st_ino == right.st_ino


def validate_parent(metadata, effective_uid):
    if not stat.S_ISDIR(metadata.st_mode):
        fail()
    if metadata.st_uid != effective_uid:
        fail()
    if stat.S_IMODE(metadata.st_mode) & 0o022:
        fail()


def validate_directory(metadata, effective_uid, effective_gid):
    if not stat.S_ISDIR(metadata.st_mode):
        fail()
    if metadata.st_uid != effective_uid:
        fail()
    if metadata.st_gid != effective_gid:
        fail()
    if stat.S_IMODE(metadata.st_mode) != 0o700:
        fail()


def validate_log_file(metadata, effective_uid, effective_gid):
    if not stat.S_ISREG(metadata.st_mode):
        fail()
    if metadata.st_uid != effective_uid:
        fail()
    if metadata.st_gid != effective_gid:
        fail()
    if stat.S_IMODE(metadata.st_mode) != 0o600:
        fail()
    if metadata.st_nlink != 1:
        fail()


def write_all(descriptor, payload):
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            fail()
        view = view[written:]


def revalidate_path_binding(parent_path, parent_fd, directory_name, directory_fd, log_name, log_fd):
    effective_uid = os.geteuid()
    effective_gid = os.getegid()
    parent_fd_metadata = os.fstat(parent_fd)
    parent_path_metadata = os.stat(parent_path, follow_symlinks=False)
    validate_parent(parent_fd_metadata, effective_uid)
    if not same_identity(parent_fd_metadata, parent_path_metadata):
        fail()

    directory_fd_metadata = os.fstat(directory_fd)
    directory_path_metadata = os.stat(
        directory_name,
        dir_fd=parent_fd,
        follow_symlinks=False,
    )
    validate_directory(directory_fd_metadata, effective_uid, effective_gid)
    if not same_identity(directory_fd_metadata, directory_path_metadata):
        fail()

    log_fd_metadata = os.fstat(log_fd)
    log_path_metadata = os.stat(log_name, dir_fd=directory_fd, follow_symlinks=False)
    validate_log_file(log_fd_metadata, effective_uid, effective_gid)
    if not same_identity(log_fd_metadata, log_path_metadata):
        fail()


def main():
    if len(sys.argv) != 5:
        fail()
    action, log_dir, log_name, test_exit_after = sys.argv[1:]
    if action not in {"prepare", "stream"}:
        fail()
    if log_name != "prod-db-backup.log":
        fail()
    if not os.path.isabs(log_dir) or os.path.normpath(log_dir) != log_dir:
        fail()

    effective_uid = os.geteuid()
    effective_gid = os.getegid()
    if effective_uid == 0 and log_dir != "/var/log/degen-prod-db-backup":
        fail()
    if test_exit_after and os.environ.get("DEGEN_BACKUP_TEST_MODE") != "1":
        fail()
    test_replace_path = os.environ.get(
        "DEGEN_BACKUP_TEST_LOGGER_REPLACE_PATH_AFTER_OPEN",
        "",
    )
    if test_replace_path not in {"", "1"}:
        fail()
    if test_replace_path and os.environ.get("DEGEN_BACKUP_TEST_MODE") != "1":
        fail()

    parent_path = os.path.dirname(log_dir)
    directory_name = os.path.basename(log_dir)
    if not parent_path or directory_name in {"", ".", ".."}:
        fail()

    directory_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | os.O_NOFOLLOW
    file_flags = (
        os.O_WRONLY
        | os.O_APPEND
        | os.O_CREAT
        | os.O_CLOEXEC
        | os.O_NOFOLLOW
        | os.O_NONBLOCK
    )
    parent_fd = None
    directory_fd = None
    log_fd = None
    try:
        parent_fd = os.open(parent_path, directory_flags)
        parent_metadata = os.fstat(parent_fd)
        validate_parent(parent_metadata, effective_uid)
        parent_path_metadata = os.stat(parent_path, follow_symlinks=False)
        if not same_identity(parent_metadata, parent_path_metadata):
            fail()

        try:
            os.mkdir(directory_name, 0o700, dir_fd=parent_fd)
        except FileExistsError:
            pass
        directory_fd = os.open(directory_name, directory_flags, dir_fd=parent_fd)
        directory_metadata = os.fstat(directory_fd)
        directory_path_metadata = os.stat(
            directory_name,
            dir_fd=parent_fd,
            follow_symlinks=False,
        )
        validate_directory(directory_metadata, effective_uid, effective_gid)
        if not same_identity(directory_metadata, directory_path_metadata):
            fail()

        log_fd = os.open(log_name, file_flags, 0o600, dir_fd=directory_fd)
        if test_replace_path:
            replaced_name = log_name + ".test-replaced"
            os.rename(
                log_name,
                replaced_name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
            )
            replacement_fd = os.open(
                log_name,
                file_flags | os.O_EXCL,
                0o600,
                dir_fd=directory_fd,
            )
            os.close(replacement_fd)
        revalidate_path_binding(
            parent_path,
            parent_fd,
            directory_name,
            directory_fd,
            log_name,
            log_fd,
        )
        if action == "prepare":
            return

        marker = test_exit_after.encode("utf-8")
        marker_tail = b""
        while True:
            payload = os.read(0, 65536)
            if not payload:
                break
            marker_window = marker_tail + payload
            marker_seen = bool(marker) and marker in marker_window
            if marker:
                tail_length = max(len(marker) - 1, 0)
                marker_tail = marker_window[-tail_length:] if tail_length else b""
            write_all(log_fd, payload)
            write_all(1, payload)
            if marker_seen:
                raise SystemExit(96)
        os.fsync(log_fd)
        revalidate_path_binding(
            parent_path,
            parent_fd,
            directory_name,
            directory_fd,
            log_name,
            log_fd,
        )
    finally:
        for descriptor in (log_fd, directory_fd, parent_fd):
            if descriptor is not None:
                os.close(descriptor)


try:
    main()
except SystemExit:
    raise
except Exception:
    sys.stderr.write("ERROR: Secure backup logging failed\n")
    raise SystemExit(96)
' "$action" "$LOG_DIR" "prod-db-backup.log" "$test_exit_after"
}


start_logging() {
    exec {original_stdout_fd}>&1 {original_stderr_fd}>&2
    coproc LOGGER_PROCESS {
        set -o pipefail
        timestamp_stream | run_secure_log_helper stream >&${original_stdout_fd}
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
    local receipt_status=0
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
    if (( rclone_receipt_started == 1 && rclone_receipt_finished == 0 )); then
        if ! finish_rclone_config_receipts; then
            receipt_status=1
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
    if (( status == 0 && receipt_status != 0 )); then
        status=$receipt_status
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
    if [[ -v BACKUP_PREFIX ]]; then
        validate_label "$BACKUP_PREFIX"
        [[ "$BACKUP_PREFIX" =~ _$ ]] || die "Invalid managed backup configuration"
    fi
}


read_lock_path_metadata() {
    local metadata owner mode links device inode
    [[ ! -L "$LOCK_FILE" && -f "$LOCK_FILE" ]] || return 1
    if ! metadata=$(stat -c '%u|%a|%h|%d|%i' -- "$LOCK_FILE" 2>/dev/null); then
        return 1
    fi
    IFS='|' read -r owner mode links device inode <<< "$metadata"
    [[ "$owner" == "$EUID" && "$links" == "1" && \
       "$mode" =~ ^[0-7]{3,4}$ && "$device" =~ ^[0-9]+$ && "$inode" =~ ^[0-9]+$ ]] || return 1
    (( (8#$mode & 0077) == 0 )) || return 1
    printf '%s\n' "$metadata"
}


read_lock_fd_metadata() {
    local fd=$1
    local fd_path="/proc/$$/fd/$fd"
    local metadata owner mode links device inode
    [[ -e "$fd_path" && -f "$fd_path" ]] || return 1
    if ! metadata=$(stat -Lc '%u|%a|%h|%d|%i' -- "$fd_path" 2>/dev/null); then
        return 1
    fi
    IFS='|' read -r owner mode links device inode <<< "$metadata"
    [[ "$owner" == "$EUID" && "$links" == "1" && \
       "$mode" =~ ^[0-7]{3,4}$ && "$device" =~ ^[0-9]+$ && "$inode" =~ ^[0-9]+$ ]] || return 1
    (( (8#$mode & 0077) == 0 )) || return 1
    printf '%s\n' "$metadata"
}


validate_lock_parent() {
    local metadata owner mode device inode
    lock_parent=${LOCK_FILE%/*}
    if [[ -z "$lock_parent" ]]; then
        lock_parent=/
    fi
    [[ "$LOCK_FILE" == /* && ! -L "$lock_parent" && -d "$lock_parent" ]] || \
        die "Invalid backup lock"
    if ! metadata=$(stat -c '%u|%a|%d|%i' -- "$lock_parent" 2>/dev/null); then
        die "Invalid backup lock"
    fi
    IFS='|' read -r owner mode device inode <<< "$metadata"
    if [[ "$owner" != "$EUID" || "$mode" != "700" || \
          ! "$device" =~ ^[0-9]+$ || ! "$inode" =~ ^[0-9]+$ ]]; then
        die "Invalid backup lock"
    fi
    lock_parent_metadata=$metadata
}


revalidate_lock_parent() {
    local metadata
    [[ ! -L "$lock_parent" && -d "$lock_parent" ]] || die "Invalid backup lock"
    if ! metadata=$(stat -c '%u|%a|%d|%i' -- "$lock_parent" 2>/dev/null); then
        die "Invalid backup lock"
    fi
    [[ "$metadata" == "$lock_parent_metadata" ]] || die "Invalid backup lock"
}


acquire_normal_lock() {
    local before_metadata=""
    local path_metadata fd_metadata final_path_metadata final_fd_metadata
    validate_lock_parent
    if [[ -e "$LOCK_FILE" || -L "$LOCK_FILE" ]]; then
        if ! before_metadata=$(read_lock_path_metadata); then
            die "Invalid backup lock"
        fi
    fi
    revalidate_lock_parent
    if ! { exec 9<>"$LOCK_FILE"; } 2>/dev/null; then
        die "Invalid backup lock"
    fi
    if ! path_metadata=$(read_lock_path_metadata) || \
       ! fd_metadata=$(read_lock_fd_metadata 9) || \
       [[ "$path_metadata" != "$fd_metadata" ]]; then
        die "Invalid backup lock"
    fi
    if [[ -n "$before_metadata" && "$before_metadata" != "$path_metadata" ]]; then
        die "Invalid backup lock"
    fi
    revalidate_lock_parent
    if ! flock -n 9 >/dev/null 2>&1; then
        die "Backup is already running; lock unavailable"
    fi
    if ! final_path_metadata=$(read_lock_path_metadata) || \
       ! final_fd_metadata=$(read_lock_fd_metadata 9) || \
       [[ "$final_path_metadata" != "$path_metadata" || \
          "$final_fd_metadata" != "$fd_metadata" || \
          "$final_path_metadata" != "$final_fd_metadata" ]]; then
        die "Invalid backup lock"
    fi
    revalidate_lock_parent
}


acquire_inherited_lock() {
    local fd=$1
    local path_metadata fd_metadata final_path_metadata final_fd_metadata
    validate_lock_parent
    if ! path_metadata=$(read_lock_path_metadata) || \
       ! fd_metadata=$(read_lock_fd_metadata "$fd") || \
       [[ "$path_metadata" != "$fd_metadata" ]]; then
        die "Invalid backup lock"
    fi
    revalidate_lock_parent
    if ! flock -n "$fd" >/dev/null 2>&1; then
        die "Backup is already running; lock unavailable"
    fi
    if ! final_path_metadata=$(read_lock_path_metadata) || \
       ! final_fd_metadata=$(read_lock_fd_metadata "$fd") || \
       [[ "$final_path_metadata" != "$path_metadata" || \
          "$final_fd_metadata" != "$fd_metadata" || \
          "$final_path_metadata" != "$final_fd_metadata" ]]; then
        die "Invalid backup lock"
    fi
    revalidate_lock_parent
}


validate_rclone_configuration() {
    local parent parent_before parent_after parent_owner parent_mode parent_device parent_inode
    local file_metadata file_owner file_mode file_links file_device file_inode
    parent=${RCLONE_CONFIG%/*}
    if [[ -z "$parent" ]]; then
        parent=/
    fi
    if [[ "$RCLONE_CONFIG" != /* || -L "$parent" || ! -d "$parent" ]]; then
        die "Invalid rclone configuration"
    fi
    if ! parent_before=$(stat -c '%u|%a|%d|%i' -- "$parent" 2>/dev/null); then
        die "Invalid rclone configuration"
    fi
    IFS='|' read -r parent_owner parent_mode parent_device parent_inode <<< "$parent_before"
    if [[ "$parent_owner" != "$EUID" || ! "$parent_mode" =~ ^[0-7]{3,4}$ || \
          ! "$parent_device" =~ ^[0-9]+$ || ! "$parent_inode" =~ ^[0-9]+$ ]] || \
       (( (8#$parent_mode & 0022) != 0 )); then
        die "Invalid rclone configuration"
    fi
    if [[ -L "$RCLONE_CONFIG" || ! -f "$RCLONE_CONFIG" || ! -r "$RCLONE_CONFIG" ]]; then
        die "Invalid rclone configuration"
    fi
    if ! file_metadata=$(stat -c '%u|%a|%h|%d|%i' -- "$RCLONE_CONFIG" 2>/dev/null); then
        die "Invalid rclone configuration"
    fi
    IFS='|' read -r file_owner file_mode file_links file_device file_inode <<< "$file_metadata"
    if [[ "$file_owner" != "$EUID" || "$file_mode" != "600" || "$file_links" != "1" || \
          ! "$file_device" =~ ^[0-9]+$ || ! "$file_inode" =~ ^[0-9]+$ ]]; then
        die "Invalid rclone configuration"
    fi
    if [[ -L "$parent" || ! -d "$parent" ]] || \
       ! parent_after=$(stat -c '%u|%a|%d|%i' -- "$parent" 2>/dev/null) || \
       [[ "$parent_after" != "$parent_before" ]]; then
        die "Invalid rclone configuration"
    fi
}


read_rclone_config_receipt() {
    python3 - "$RCLONE_CONFIG" 2>/dev/null <<'PY'
import hashlib
import os
import stat
import sys


path = sys.argv[1]
parent_path = os.path.dirname(path) or "/"
parent_before = os.lstat(parent_path)
if (
    not stat.S_ISDIR(parent_before.st_mode)
    or parent_before.st_uid != os.geteuid()
    or stat.S_IMODE(parent_before.st_mode) & 0o022
):
    raise SystemExit(1)

try:
    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
except AttributeError:
    raise SystemExit(1)
fd = os.open(path, flags)
try:
    before = os.fstat(fd)
    path_before = os.lstat(path)
    if (
        not stat.S_ISREG(before.st_mode)
        or stat.S_ISLNK(path_before.st_mode)
        or (before.st_dev, before.st_ino) != (path_before.st_dev, path_before.st_ino)
        or before.st_uid != os.geteuid()
        or stat.S_IMODE(before.st_mode) != 0o600
        or before.st_nlink != 1
    ):
        raise SystemExit(1)
    digest = hashlib.sha256()
    while True:
        chunk = os.read(fd, 1024 * 1024)
        if not chunk:
            break
        digest.update(chunk)
    after = os.fstat(fd)
    path_after = os.lstat(path)
    parent_after = os.lstat(parent_path)
    stable_fields = (
        "st_dev",
        "st_ino",
        "st_uid",
        "st_gid",
        "st_mode",
        "st_nlink",
        "st_size",
        "st_mtime_ns",
        "st_ctime_ns",
    )
    if (
        any(getattr(before, field) != getattr(after, field) for field in stable_fields)
        or (after.st_dev, after.st_ino) != (path_after.st_dev, path_after.st_ino)
        or any(
            getattr(parent_before, field) != getattr(parent_after, field)
            for field in ("st_dev", "st_ino", "st_uid", "st_gid", "st_mode")
        )
    ):
        raise SystemExit(1)
    print(
        f"sha256={digest.hexdigest()} "
        f"device={after.st_dev} inode={after.st_ino} "
        f"uid={after.st_uid} gid={after.st_gid} "
        f"mode={stat.S_IMODE(after.st_mode):04o} links={after.st_nlink} "
        f"size={after.st_size} mtime_ns={after.st_mtime_ns}"
    )
finally:
    os.close(fd)
PY
}


begin_rclone_config_receipts() {
    local metadata hash mtime_ns
    if ! metadata=$(read_rclone_config_receipt); then
        die "Unable to record rclone configuration metadata"
    fi
    hash=${metadata#sha256=}
    hash=${hash%% *}
    mtime_ns=${metadata##* mtime_ns=}
    if [[ ! "$hash" =~ ^[0-9a-f]{64}$ || ! "$mtime_ns" =~ ^[0-9]+$ ]]; then
        die "Unable to record rclone configuration metadata"
    fi
    if ! log "RCLONE_CONFIG_RECEIPT phase=before status=ok path=$RCLONE_CONFIG $metadata change=baseline"; then
        die "Unable to log rclone configuration metadata"
    fi
    rclone_receipt_before_metadata=$metadata
    rclone_receipt_started=1
}


finish_rclone_config_receipts() {
    local metadata hash mtime_ns change
    if (( rclone_receipt_finished == 1 )); then
        return 0
    fi
    if ! metadata=$(read_rclone_config_receipt); then
        rclone_receipt_finished=1
        log "RCLONE_CONFIG_RECEIPT phase=final status=error reason=metadata-unavailable" || true
        return 1
    fi
    hash=${metadata#sha256=}
    hash=${hash%% *}
    mtime_ns=${metadata##* mtime_ns=}
    if [[ ! "$hash" =~ ^[0-9a-f]{64}$ || ! "$mtime_ns" =~ ^[0-9]+$ ]]; then
        rclone_receipt_finished=1
        log "RCLONE_CONFIG_RECEIPT phase=final status=error reason=metadata-unavailable" || true
        return 1
    fi
    change=unchanged
    if [[ "$metadata" != "$rclone_receipt_before_metadata" ]]; then
        change=possible-oauth-refresh
    fi
    if ! log "RCLONE_CONFIG_RECEIPT phase=final status=ok path=$RCLONE_CONFIG $metadata change=$change"; then
        rclone_receipt_finished=1
        return 1
    fi
    rclone_receipt_finished=1
}


require_tools() {
    local tool
    local -a tools=(
        python3 psql pg_dump pg_restore sha256sum stat df awk flock rclone
        hostname date mktemp mv rm
    )
    for tool in "${tools[@]}"; do
        command -v "$tool" >/dev/null 2>&1 || die "Required tool is unavailable: $tool"
    done
    [[ -x "$RETENTION_PLANNER" ]] || die "Retention planner is not executable: $RETENTION_PLANNER"
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
    local database_name host expected_prefix
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
    expected_prefix="${database_name}_${host}_"
    if [[ -v BACKUP_PREFIX && "$BACKUP_PREFIX" != "$expected_prefix" ]]; then
        die "Configured backup prefix does not match live database and host identity"
    fi
    backup_prefix=$expected_prefix
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
    validate_rclone_configuration
    require_tools
    validate_backup_directory
    load_database_url
    derive_backup_prefix
    begin_rclone_config_receipts
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

    if ! rclone --config "$RCLONE_CONFIG" --ignore-existing --error-on-no-transfer \
        copyto "$dump_path" "$owned_remote_dump_temp" >/dev/null 2>&1; then
        log "Remote dump temp was not cleanup-owned after failed upload and remains protected: $temp_dump_name"
        die "Remote dump temporary upload failed"
    fi
    owns_remote_dump_temp=1
    if ! rclone --config "$RCLONE_CONFIG" --ignore-existing --error-on-no-transfer \
        copyto "$sidecar_path" "$owned_remote_sidecar_temp" >/dev/null 2>&1; then
        log "Remote checksum temp was not cleanup-owned after failed upload and remains protected: $temp_sidecar_name"
        die "Remote checksum temporary upload failed"
    fi
    owns_remote_sidecar_temp=1

    verify_remote_size "$owned_remote_dump_temp" "$local_size" "Temporary"
    verify_remote_sidecar "$owned_remote_sidecar_temp" "$checksum" "$dump_name" "Temporary"

    load_remote_inventory remote_inventory
    assert_remote_names_absent remote_inventory "Pre-publish" "$dump_name" "$sidecar_name"

    if ! rclone --config "$RCLONE_CONFIG" --ignore-existing --error-on-no-transfer \
        moveto "$owned_remote_dump_temp" "$final_dump" >/dev/null 2>&1; then
        die "Remote dump publish move failed"
    fi
    load_remote_inventory remote_inventory
    assert_remote_names_absent remote_inventory "Post-move temp source" "$temp_dump_name"
    owns_remote_dump_temp=0
    if ! rclone --config "$RCLONE_CONFIG" --ignore-existing --error-on-no-transfer \
        moveto "$owned_remote_sidecar_temp" "$final_sidecar" >/dev/null 2>&1; then
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
    local path basename remainder
    local had_dotglob=0
    local had_nullglob=0
    destination=()
    if shopt -q dotglob; then
        had_dotglob=1
    fi
    if shopt -q nullglob; then
        had_nullglob=1
    fi
    shopt -s dotglob nullglob
    for path in "$BACKUP_DIR"/*; do
        basename=${path##*/}
        if [[ -L "$path" ]]; then
            remainder=${basename#"$backup_prefix"}
            if [[ "$basename" == "$backup_prefix"* && "$remainder" =~ ^[0-9]{8}T[0-9]{6}Z\.dump(\.sha256)?$ ]]; then
                die "Unsafe local backup inventory entry: $basename is a symlink"
            fi
            continue
        fi
        [[ -f "$path" ]] || continue
        destination+=("$basename")
    done
    if (( had_dotglob == 0 )); then
        shopt -u dotglob
    fi
    if (( had_nullglob == 0 )); then
        shopt -u nullglob
    fi
}


validate_retention_candidates() {
    local now=$1
    local inventory_name=$2
    local candidates_name=$3
    local candidate_role=$4
    local candidate remainder stamp inventory_item counterpart
    local -n inventory_values=$inventory_name
    local -n candidate_values=$candidates_name
    local -A inventory_set=() seen_candidates=() candidate_set=()

    for inventory_item in "${inventory_values[@]}"; do
        if [[ -n "$inventory_item" && "$inventory_item" != */* && \
              "$inventory_item" =~ ^[A-Za-z0-9._-]+$ && \
              "$inventory_item" == "$backup_prefix"* ]]; then
            remainder=${inventory_item#"$backup_prefix"}
            if [[ "$remainder" =~ ^[0-9]{8}T[0-9]{6}Z\.dump(\.sha256)?$ ]]; then
                inventory_set["$inventory_item"]=1
            fi
        fi
    done

    for candidate in "${candidate_values[@]}"; do
        if [[ -z "$candidate" || "$candidate" == */* || ! "$candidate" =~ ^[A-Za-z0-9._-]+$ || "$candidate" != "$backup_prefix"* ]]; then
            die "Unsafe retention candidate: $candidate"
        fi
        remainder=${candidate#"$backup_prefix"}
        if [[ ! "$remainder" =~ ^[0-9]{8}T[0-9]{6}Z\.dump(\.sha256)?$ ]]; then
            die "Unsafe retention candidate: $candidate"
        fi
        stamp=${remainder%%.dump*}
        if [[ "$stamp" > "$now" ]]; then
            die "Unsafe retention candidate: $candidate is future dated"
        fi
        if [[ "$candidate_role" == "delete" && "$stamp" == "$now" ]]; then
            die "Unsafe retention candidate: $candidate is current dated"
        fi

        [[ -n "${inventory_set[$candidate]+present}" ]] || \
            die "Unsafe retention candidate: $candidate was not in the planned inventory"
        [[ -z "${seen_candidates[$candidate]+present}" ]] || \
            die "Unsafe retention candidate: duplicate $candidate"
        seen_candidates["$candidate"]=1
        candidate_set["$candidate"]=1
    done

    for candidate in "${candidate_values[@]}"; do
        if [[ "$candidate" == *.sha256 ]]; then
            counterpart=${candidate%.sha256}
        else
            counterpart="${candidate}.sha256"
        fi
        [[ -n "${candidate_set[$counterpart]+present}" ]] || \
            die "Unsafe retention candidate: $candidate was not emitted as a complete pair"
    done
}


filter_retention_planner_output() {
    local max_records=$1
    local max_line_bytes=$2
    python3 -c '
import os
import sys


max_records = int(sys.argv[1])
max_line_bytes = int(sys.argv[2])
pending = bytearray()
record_count = 0


def fail(reason):
    print(f"retention planner output {reason}", file=sys.stderr)
    raise SystemExit(65)


def write_all(payload):
    offset = 0
    while offset < len(payload):
        offset += os.write(1, payload[offset:])


while True:
    chunk = os.read(0, 65536)
    if not chunk:
        break
    if b"\x00" in chunk or b"\r" in chunk:
        fail("contained a forbidden byte")
    start = 0
    while True:
        newline = chunk.find(b"\n", start)
        if newline < 0:
            tail = chunk[start:]
            if len(pending) + len(tail) > max_line_bytes:
                fail("exceeded safe line length")
            pending.extend(tail)
            break
        segment = chunk[start:newline]
        if len(pending) + len(segment) > max_line_bytes:
            fail("exceeded safe line length")
        pending.extend(segment)
        if not pending:
            fail("contained an empty record")
        record_count += 1
        if record_count > max_records:
            fail("exceeded safe record count")
        write_all(pending + b"\n")
        pending.clear()
        start = newline + 1

if pending:
    fail("was not LF-terminated")
' "$max_records" "$max_line_bytes"
}


planner_candidates() {
    local mode=$1
    local now=$2
    local inventory_name=$3
    local output_name=$4
    local output_format=$5
    local -n inventory_ref=$inventory_name
    local -n output_ref=$output_name
    local planner_output candidate_role inventory_item remainder candidate_bytes
    local max_records=${#inventory_ref[@]}
    local max_candidate_bytes=0
    local -a policy

    case "$output_format" in
        keep-names) candidate_role=keep ;;
        delete-names) candidate_role=delete ;;
        *) die "Unsafe retention planner output format: $output_format" ;;
    esac
    output_ref=()

    if [[ "$mode" == "local" ]]; then
        policy=(--local-count "$KEEP_LOCAL_COUNT")
    else
        policy=(
            --daily "$KEEP_REMOTE_DAILY"
            --weekly "$KEEP_REMOTE_WEEKLY"
            --monthly "$KEEP_REMOTE_MONTHLY"
        )
    fi
    for inventory_item in "${inventory_ref[@]}"; do
        if [[ -n "$inventory_item" && "$inventory_item" != */* && \
              "$inventory_item" =~ ^[A-Za-z0-9._-]+$ && \
              "$inventory_item" == "$backup_prefix"* ]]; then
            remainder=${inventory_item#"$backup_prefix"}
            if [[ "$remainder" =~ ^[0-9]{8}T[0-9]{6}Z\.dump(\.sha256)?$ ]]; then
                candidate_bytes=${#inventory_item}
                if (( candidate_bytes > max_candidate_bytes )); then
                    max_candidate_bytes=$candidate_bytes
                fi
            fi
        fi
    done
    if ! planner_output=$(
        printf '%s\n' "${inventory_ref[@]}" |
            "$RETENTION_PLANNER" \
                --mode "$mode" \
                --prefix "$backup_prefix" \
                --now "$now" \
                "${policy[@]}" \
                --format "$output_format" |
            filter_retention_planner_output "$max_records" "$max_candidate_bytes"
    ); then
        die "Retention planner failed for mode=$mode"
    fi
    if [[ -n "$planner_output" ]]; then
        mapfile -t output_ref <<< "$planner_output"
    fi
    validate_retention_candidates "$now" "$inventory_name" "$output_name" "$candidate_role"
}


validate_local_retention_plan() {
    local now=$1
    local keep_name=$2
    local delete_name=$3
    local current_dump current_sidecar candidate
    local -n keep_values=$keep_name
    local -n delete_values=$delete_name
    local -A keep_set=()

    for candidate in "${keep_values[@]}"; do
        keep_set["$candidate"]=1
    done
    current_dump="${backup_prefix}${now}.dump"
    current_sidecar="${current_dump}.sha256"
    for candidate in "$current_dump" "$current_sidecar"; do
        [[ -n "${keep_set[$candidate]+present}" ]] || \
            die "Unsafe retention plan: current backup pair was not kept"
    done

    for candidate in "${delete_values[@]}"; do
        [[ -z "${keep_set[$candidate]+present}" ]] || \
            die "Unsafe retention plan: keep and delete outputs overlap at $candidate"
    done
}


validate_retained_local_pair() {
    local dump_name=$1
    local sidecar_name="${dump_name}.sha256"
    # The root-owned backup directory and held operation lock are the trust boundary.
    # Descriptor checks detect replacement and symlink mistakes without claiming safety
    # against a malicious root process that can mutate the directory concurrently.
    if ! python3 - "$BACKUP_DIR" "$dump_name" "$sidecar_name" <<'PY'
import os
import re
import stat
import subprocess
import sys


directory, dump_name, sidecar_name = sys.argv[1:]
opened = []
required_flags = ("O_NOFOLLOW", "O_DIRECTORY")


def fail(message):
    print(f"retained backup verification error: {message}", file=sys.stderr)
    raise SystemExit(1)


def signature(value):
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_nlink,
        value.st_uid,
        value.st_gid,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def open_regular_at(directory_fd, name):
    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
    descriptor = os.open(name, flags, dir_fd=directory_fd)
    opened.append(descriptor)
    metadata = os.fstat(descriptor)
    if not stat.S_ISREG(metadata.st_mode):
        fail(f"{name} is not a regular file")
    if metadata.st_uid != os.geteuid():
        fail(f"{name} is not owned by the effective user")
    if stat.S_IMODE(metadata.st_mode) != 0o600:
        fail(f"{name} does not have exact mode 0600")
    if metadata.st_nlink != 1:
        fail(f"{name} has an unsafe link count")
    return descriptor, metadata


def read_descriptor(descriptor):
    os.lseek(descriptor, 0, os.SEEK_SET)
    chunks = []
    while True:
        chunk = os.read(descriptor, 1024 * 1024)
        if not chunk:
            return b"".join(chunks)
        chunks.append(chunk)


def require_stable(descriptor, before, label):
    after = os.fstat(descriptor)
    if signature(before) != signature(after):
        fail(f"{label} changed while it was being verified")


def require_path_identity(directory_fd, name, descriptor):
    path_metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    descriptor_metadata = os.fstat(descriptor)
    if not stat.S_ISREG(path_metadata.st_mode):
        fail(f"{name} is no longer a regular file")
    if (path_metadata.st_dev, path_metadata.st_ino) != (
        descriptor_metadata.st_dev,
        descriptor_metadata.st_ino,
    ):
        fail(f"{name} was replaced while it was being verified")


try:
    missing_flags = [name for name in required_flags if not hasattr(os, name)]
    if missing_flags:
        fail(f"required open flags unavailable: {', '.join(missing_flags)}")
    directory_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | os.O_NOFOLLOW
    directory_fd = os.open(directory, directory_flags)
    opened.append(directory_fd)
    dump_fd, dump_metadata = open_regular_at(directory_fd, dump_name)
    sidecar_fd, sidecar_metadata = open_regular_at(directory_fd, sidecar_name)

    expected_sidecar_size = 64 + 2 + len(os.fsencode(dump_name)) + 1
    if sidecar_metadata.st_size != expected_sidecar_size:
        fail(f"{sidecar_name} had an invalid byte length")
    sidecar = read_descriptor(sidecar_fd)
    require_stable(sidecar_fd, sidecar_metadata, sidecar_name)
    sidecar_match = re.fullmatch(
        rb"([0-9a-f]{64})  " + re.escape(os.fsencode(dump_name)) + rb"\n",
        sidecar,
    )
    if sidecar_match is None:
        fail(f"{sidecar_name} did not contain one exact checksum record")

    dump_descriptor_path = f"/proc/self/fd/{dump_fd}"
    os.lseek(dump_fd, 0, os.SEEK_SET)
    checksum = subprocess.run(
        ["sha256sum", "--", dump_descriptor_path],
        pass_fds=(dump_fd,),
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    checksum_match = re.fullmatch(rb"([0-9a-f]{64})  [^\r\n]+\n", checksum.stdout)
    if checksum.returncode != 0 or checksum_match is None:
        fail(f"sha256sum failed for {dump_name}")
    require_stable(dump_fd, dump_metadata, dump_name)
    if checksum_match.group(1) != sidecar_match.group(1):
        fail(f"checksum mismatch for {dump_name}")

    os.lseek(dump_fd, 0, os.SEEK_SET)
    restore = subprocess.run(
        ["pg_restore", "--list", dump_descriptor_path],
        pass_fds=(dump_fd,),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if restore.returncode != 0:
        fail(f"pg_restore validation failed for {dump_name}")
    require_stable(dump_fd, dump_metadata, dump_name)
    require_stable(sidecar_fd, sidecar_metadata, sidecar_name)
    require_path_identity(directory_fd, dump_name, dump_fd)
    require_path_identity(directory_fd, sidecar_name, sidecar_fd)
except OSError as exc:
    fail(exc.strerror or "filesystem validation failed")
finally:
    for descriptor in reversed(opened):
        try:
            os.close(descriptor)
        except OSError:
            pass
PY
    then
        die "Retained local backup validation failed: $dump_name"
    fi
}


run_local_retention() {
    local now=$1
    local candidate current_dump
    local -a inventory=() keep_candidates=() delete_candidates=()
    collect_local_names inventory
    planner_candidates local "$now" inventory keep_candidates keep-names
    planner_candidates local "$now" inventory delete_candidates delete-names
    validate_local_retention_plan "$now" keep_candidates delete_candidates

    current_dump="${backup_prefix}${now}.dump"
    for candidate in "${keep_candidates[@]}"; do
        if [[ "$candidate" != *.sha256 && "$candidate" != "$current_dump" ]]; then
            validate_retained_local_pair "$candidate"
        fi
    done

    if (( ${#delete_candidates[@]} == 0 )); then
        log "Local retention candidates: none"
        return
    fi
    for candidate in "${delete_candidates[@]}"; do
        log "Local retention candidate: $candidate"
    done
    for candidate in "${delete_candidates[@]}"; do
        [[ -f "$BACKUP_DIR/$candidate" && ! -L "$BACKUP_DIR/$candidate" ]] || \
            die "Unsafe retention candidate changed before delete: $candidate"
    done
    for candidate in "${delete_candidates[@]}"; do
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
    planner_candidates remote "$now" inventory candidates delete-names
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


parse_arguments() {
    mode=run
    inherited_lock_fd=""
    requested_now=""
    case $# in
        0)
            ;;
        1)
            mode=$1
            ;;
        3)
            mode=$1
            if [[ "$2" != "--lock-fd" || ! "$3" =~ ^[0-9]+$ ]]; then
                die "Unsupported mode or extra arguments"
            fi
            inherited_lock_fd=$3
            ;;
        5)
            mode=$1
            if [[ "$2" != "--lock-fd" || ! "$3" =~ ^[0-9]+$ || \
                  "$4" != "--now" || ! "$5" =~ ^[0-9]{8}T[0-9]{6}Z$ ]]; then
                die "Unsupported mode or extra arguments"
            fi
            inherited_lock_fd=$3
            requested_now=$5
            ;;
        *)
            die "Unsupported mode or extra arguments"
            ;;
    esac
    case "$mode" in
        run|preflight|remote-retention-dry-run) ;;
        *) die "Unsupported mode: $mode" ;;
    esac
    if [[ -n "$inherited_lock_fd" && "$mode" == "run" ]]; then
        die "--lock-fd is not permitted for run mode"
    fi
    if [[ -n "$requested_now" ]]; then
        [[ "$mode" == "remote-retention-dry-run" ]] || \
            die "--now is permitted only for remote-retention-dry-run mode"
        local parsed_now
        if ! parsed_now=$(
            /usr/bin/date -u \
                --date="${requested_now:0:8} ${requested_now:9:2}:${requested_now:11:2}:${requested_now:13:2} UTC" \
                '+%Y%m%dT%H%M%SZ' 2>/dev/null
        ); then
            die "--now must be a valid UTC timestamp"
        fi
        [[ "$parsed_now" == "$requested_now" ]] || \
            die "--now must be a valid UTC timestamp"
    fi
}


main() {
    local now remote_delete_allowed
    parse_arguments "$@"
    resolve_runtime_configuration_paths
    load_managed_configuration
    validate_configuration

    mkdir -p -- "$BACKUP_DIR"
    run_secure_log_helper prepare </dev/null >/dev/null
    trap cleanup_on_exit EXIT
    trap 'exit 129' HUP
    trap 'exit 130' INT
    trap 'exit 143' TERM
    trap '' PIPE
    start_logging

    if [[ -n "$inherited_lock_fd" ]]; then
        acquire_inherited_lock "$inherited_lock_fd"
    else
        acquire_normal_lock
    fi

    run_preflight "$mode"
    if [[ "$mode" == "preflight" ]]; then
        log "Preflight completed; no dump or retention was performed"
        return
    fi

    if [[ -n "$requested_now" ]]; then
        now=$requested_now
    else
        now=$(date -u '+%Y%m%dT%H%M%SZ')
    fi
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


main "$@"
