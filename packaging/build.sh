#!/bin/bash -e

# =-=-=-=-=-=-=-
# detect environment
SCRIPTNAME=`basename $0`
SCRIPTPATH=$( cd $(dirname $0) ; pwd -P )
FULLPATHSCRIPTNAME=$SCRIPTPATH/$SCRIPTNAME
BUILDDIR=$( cd $SCRIPTPATH/../ ; pwd -P )
cd $SCRIPTPATH

# =-=-=-=-=-=-=-
# check input
USAGE="
Usage:
  $SCRIPTNAME
  $SCRIPTNAME clean
"
if [ $# -gt 1 -o "$1" == "-h" -o "$1" == "--help" -o "$1" == "help" ] ; then
    echo "$USAGE"
    exit 1
fi

# =-=-=-=-=-=-=-
# require irods-dev package
if [ ! -d /usr/include/irods ] ; then
    echo "ERROR :: \"irods-dev\" package required to build this plugin" 1>&2
    exit 1
fi

# =-=-=-=-=-=-=-
# handle the case of build clean
if [ "$1" == "clean" ] ; then
    rm -f $SCRIPTPATH/irods_resource_plugin_*.list
    cd $BUILDDIR
    rm -rf linux-*
    make clean
    exit 0
fi

# =-=-=-=-=-=-=-
# set up some variables
RESC_TYPE=wos

# prepare list file from template
source $SCRIPTPATH/VERSION
echo "Detected Plugin Version to Build [$PLUGINVERSION]"
echo "Detected Plugin Version Integer  [$PLUGINVERSIONINT]"
LISTFILE="$SCRIPTPATH/irods_resource_plugin_${RESC_TYPE}.list"
TMPFILE="/tmp/irods_db_plugin.list"
sed -e "s,TEMPLATE_PLUGINVERSIONINT,$PLUGINVERSIONINT," $LISTFILE.template > $TMPFILE
mv $TMPFILE $LISTFILE
sed -e "s,TEMPLATE_PLUGINVERSION,$PLUGINVERSION," $LISTFILE > $TMPFILE
mv $TMPFILE $LISTFILE

# =-=-=-=-=-=-=-
# determine the OS Flavor
DETECTEDOS=`$BUILDDIR/packaging/find_os.sh`
if [ "$PORTABLE" == "1" ] ; then
  DETECTEDOS="Portable"
fi
echo "Detected OS [$DETECTEDOS]"

# =-=-=-=-=-=-=-
# determine the OS Version
DETECTEDOSVERSION=`$BUILDDIR/packaging/find_os_version.sh`
echo "Detected OS Version [$DETECTEDOSVERSION]"

# =-=-=-=-=-=-=-
# build it
cd $BUILDDIR
echo "build dir   [$BUILDDIR]"
echo "script path [$SCRIPTPATH]"
make 

# =-=-=-=-=-=-=-
# package the plugin and associated files
if [ "$COVERAGE" == "1" ] ; then
    # sets EPM to not strip binaries of debugging information
    EPMOPTS="-g"
    # sets listfile coverage options
    EPMOPTS="$EPMOPTS COVERAGE=true"
else
    EPMOPTS=""
fi

# =-=-=-=-=-=-=-
# determine appropriate architecture
unamem=`uname -m`
if [[ "$unamem" == "x86_64" || "$unamem" == "amd64" ]] ; then
    arch="amd64"
else
    arch="i386"
fi

cd $BUILDDIR
mkdir -p build
EPMCMD="/usr/bin/epm"
if [ "$DETECTEDOS" == "RedHatCompatible" ] ; then # CentOS and RHEL and Fedora
    echo "${text_green}${text_bold}Running EPM :: Generating $DETECTEDOS RPMs${text_reset}"
    epmvar="REDHAT"
    ostype=`awk '{print $1}' /etc/redhat-release`
    osversion=`awk '{print $3}' /etc/redhat-release`
    if [ "$ostype" == "CentOS" -a "$osversion" \> "6" ]; then
        epmosversion="CENTOS6"
        SUFFIX=redhat
    else
        epmosversion="NOTCENTOS6"
        SUFFIX=centos6
    fi
    $EPMCMD $EPMOPTS -f rpm irods-resource-plugin-${RESC_TYPE} $epmvar=true $epmosversion=true $LISTFILE
    cp linux-*/irods-resource-plugin-${RESC_TYPE}*.rpm build/irods-resource-plugin-${RESC_TYPE}-${SUFFIX}.rpm

elif [ "$DETECTEDOS" == "SuSE" ] ; then # SuSE
    echo "${text_green}${text_bold}Running EPM :: Generating $DETECTEDOS RPMs${text_reset}"
    epmvar="SUSE"
    $EPMCMD $EPMOPTS -f rpm irods-resource-plugin-${RESC_TYPE} $epmvar=true $LISTFILE
    cp linux-*/irods-resource-plugin-${RESC_TYPE}*.rpm build/irods-resource-plugin-${RESC_TYPE}-suse.rpm
elif [ "$DETECTEDOS" == "Ubuntu" -o "$DETECTEDOS" == "Debian" ] ; then  # Ubuntu
    echo "${text_green}${text_bold}Running EPM :: Generating $DETECTEDOS DEBs${text_reset}"
    epmvar="DEB"
    $EPMCMD $EPMOPTS -a $arch -f deb irods-resource-plugin-${RESC_TYPE} $epmvar=true $LISTFILE
    cp linux-*/irods-resource-plugin-${RESC_TYPE}*.deb build/irods-resource-plugin-${RESC_TYPE}.deb
elif [ "$DETECTEDOS" == "Solaris" ] ; then  # Solaris
    echo "${text_green}${text_bold}Running EPM :: Generating $DETECTEDOS PKGs${text_reset}"
    epmvar="PKG"
    $EPMCMD $EPMOPTS -f pkg irods-resource-plugin-${RESC_TYPE} $epmvar=true $LISTFILE
elif [ "$DETECTEDOS" == "MacOSX" ] ; then  # MacOSX
    echo "${text_green}${text_bold}Running EPM :: Generating $DETECTEDOS DMGs${text_reset}"
    epmvar="OSX"
    $EPMCMD $EPMOPTS -f osx irods-resource-plugin-${RESC_TYPE} $epmvar=true $LISTFILE
elif [ "$DETECTEDOS" == "ArchLinux" ] ; then  # ArchLinux
    echo "${text_green}${text_bold}Running EPM :: Generating $DETECTEDOS TGZs${text_reset}"
    epmvar="ARCH"
    $EPMCMD $EPMOPTS -f portable irods-resource-plugin-${RESC_TYPE} $epmvar=true $LISTFILE
elif [ "$DETECTEDOS" == "Portable" ] ; then  # Portable
    echo "${text_green}${text_bold}Running EPM :: Generating $DETECTEDOS TGZs${text_reset}"
    epmvar="PORTABLE"
    $EPMCMD $EPMOPTS -f portable irods-resource-plugin-${RESC_TYPE} $epmvar=true $LISTFILE
else
    echo "${text_red}#######################################################" 1>&2
    echo "ERROR :: Unknown OS, cannot generate packages with EPM" 1>&2
    echo "#######################################################${text_reset}" 1>&2
    exit 1
fi
