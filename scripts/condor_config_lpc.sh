export CONDOR_CONFIG="${HOME}/.condor/condor_config"
grep -v "^include" "/etc/condor/config.d/01_cmslpc_interactive" > "${CONDOR_CONFIG}"
get_host(){
    if [ -z "${1}"]
    then
        local host="$(grep "^SCHEDD_HOST" "/storage/local/data1/condor/config.d/${USER}.config")"
    else
        local host="SCHEDD_HOST=${1}"
    fi
    echo "${host}"
}
echo "$(get_host ${1})" >> "${CONDOR_CONFIG}"