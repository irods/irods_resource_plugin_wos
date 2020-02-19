


// =-=-=-=-=-=-=-
// includes for the curl implementation
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <getopt.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>
#include <errno.h>
#include <curl/curl.h>
#include <sys/stat.h>

// =-=-=-=-=-=-=-
// irods includes
#include "rsRegReplica.hpp"
#include "rsModDataObjMeta.hpp"
#include "irods_server_properties.hpp"


// =-=-=-=-=-=-=-
// wos includes
#include "curlWosFunctions.h"

// =-=-=-=-=-=-=-
// irods includes
#include "irods_resource_plugin.hpp"
#include "irods_file_object.hpp"
#include "irods_physical_object.hpp"
#include "irods_collection_object.hpp"
#include "irods_string_tokenize.hpp"
#include "irods_hierarchy_parser.hpp"
#include "irods_resource_redirect.hpp"
#include "irods_stacktrace.hpp"
#include "irods_virtual_path.hpp"
#include "irods_kvp_string_parser.hpp"
#include "rsRegDataObj.hpp"

// =-=-=-=-=-=-=-
// stl includes
#include <iostream>
#include <sstream>
#include <vector>
#include <string>
#include "boost/lexical_cast.hpp"

#ifdef USING_JSON
#include <json/json.h>
#endif

const size_t MAX_RETRY_COUNT = 100;
size_t RETRY_COUNT = 3;

const size_t MAX_CONNECT_TIMEOUT = 120;
size_t CONNECT_TIMEOUT = 60;

static const std::string CONNECT_TIMEOUT_KEY( "connect_timeout" );
static const std::string NUM_RETRIES_KEY( "retry_count" );
static const std::string WOS_HOST_KEY( "wos_host" );
static const std::string WOS_POLICY_KEY( "wos_policy" );
static const std::string REPL_POLICY_KEY( "repl_policy" );
static const std::string REPL_POLICY_REG( "consider_wos_repl" );

// locally define the interface version function in order to
// no longer need to link against the irods client interface
double get_plugin_interface_version() {
static const double PLUGIN_INTERFACE_VERSION = 1.0;
    return PLUGIN_INTERFACE_VERSION;
}



/** 
 * @brief This function parses the headers returned from the libcurl call.
 *
 *  This function conforms to the prototype required by libcurl for 
 *  a header callback function.  It parses the headers we are interested
 *  in into a structure of type WOS_HEADERS.
 *
 * @param ptr A void ptr to the header data
 * @param size The size of single item in the header data: seems to always be 1
 * @param nmemb The number of items in the header data.
 * @param stream A pointer to the user provided data: in this case a pointer to
 *        the WOS_HEADERS structure the function fills in.
 * @return The number of bytes processed.
 */
size_t 
readTheHeaders(void *ptr, size_t size, size_t nmemb, void *stream) {
   //char *theHeader = (char *) calloc(size, nmemb + 1);
   char *theHeader = (char *) calloc(nmemb + 1, size);
   int   x_ddn_status;
   char  x_ddn_status_string[WOS_STATUS_LENGTH];
   long  x_ddn_length;
   WOS_HEADERS_P theHeaders;
 
   theHeaders = (WOS_HEADERS_P) stream;

   // We have a minus 2 as the number of bytes to copy
   // because the headers have a \r\n tacked on to the end
   // that we don't need or want. Remember that we used calloc 
   // for the space, so the string is properly terminated.
   strncpy(theHeader, (char *) ptr, ((size * nmemb)) - 2);

   // if HTTP/ appears in the string we need to check the code
   std::string h_str( theHeader );
   if( std::string::npos != h_str.find( "HTTP/" ) ) {
       if( std::string::npos != h_str.find( "50" ) ) {
           rodsLog( 
               LOG_ERROR, 
               "readTheHeaders - setting an error [%s]",
               h_str.c_str() );
           theHeaders->http_status = WOS_INTERNAL_ERROR;
       }
   }

   // Now lets see if this is a header we care about
   if (!strncasecmp(theHeader, 
                    WOS_STATUS_HEADER, 
                    strlen(WOS_STATUS_HEADER))) {
      // Time for a little pointer arithmetic: we start the
      // sscanf after the header by adding the size of the header
      // to the address of theHeader.
      sscanf(theHeader + sizeof(WOS_STATUS_HEADER), 
             "%d %s", &x_ddn_status, x_ddn_status_string);
      theHeaders->x_ddn_status = x_ddn_status;

      // Justin - Changed this to get the real status
      //strcpy(theHeaders->x_ddn_status_string, "OK" );//x_ddn_status_string);
      strcpy(theHeaders->x_ddn_status_string, x_ddn_status_string);
   } 

   if (!strncasecmp(theHeader, 
                    WOS_OID_HEADER, 
                    strlen(WOS_OID_HEADER))) {
      // Time for a little pointer arithmetic: we start the
      // sscanf after the header by adding the size of the header
      // to the address of theHeader.
      theHeaders->x_ddn_oid = (char *) calloc (strlen(theHeader), 1);
      //   (char *) calloc (strlen(WOS_STATUS_HEADER) - sizeof(WOS_OID_HEADER), 1);
      sscanf(theHeader + sizeof(WOS_OID_HEADER), 
             "%s", theHeaders->x_ddn_oid);
   } 

   if (!strncasecmp(theHeader, 
                    WOS_LENGTH_HEADER, 
                    strlen(WOS_LENGTH_HEADER))) {
      // Time for a little pointer arithmetic: we start the
      // sscanf after the header by adding the size of the header
      // to the address of theHeader.
      sscanf(theHeader + sizeof(WOS_LENGTH_HEADER), 
             "%ld", &x_ddn_length);
      theHeaders->x_ddn_length = x_ddn_length;

      // Justin - removed the following at DDN request while troubleshooting
      //strcpy(theHeaders->x_ddn_status_string, "OK" );//x_ddn_status_string);
   } 

   free(theHeader);
   return (nmemb * size);
}

/**
 * Important note about the following code. It's not currently used
 * and it's probably not going to be.  It hasn't been removed because
 * the there is no other way to provide this functionality in the current
 * DDN rest interface. DDN is, in theory, going to update the rest
 * interface to provide a stat operation.  When that happens, we'll add
 * a curl call for that interface and delete this code.  Until then, it
 * makes sense to leave this here. Making this json version of the code
 * work correctly would require enhancing the iRODS resource code to store
 * and provide an admin user, password and url.
 */
#if 0
/** 
 * @brief This function writes the data received from the DDN unit to a 
 *        memory buffer. It's used by the status operation.
 *
 *  This function conforms to the prototype required by libcurl for a 
 *  a CURLOPT_WRITEFUNCTION callback function. It writes the date returned
 *  by the curl library call to the WOS_MEMORY pointer defined in the stream
 *  parameter. Note that the function will could be called more than once,
 *  as libcurl defines a maximum buffer size.  This maximum can be
 *  redefined by recompiling libcurl.
 *
 * @param ptr A void ptr to the data to be written to disk.
 * @param size The size of a single item in the data: seems to always be 1
 * @param nmemb The number of items in the data.
 * @param stream A pointer to the user provided data: in this case a pointer to
 *        the WOS_MEMORY struct to which the data will be written.
 * @return The number of bytes added to the data on this invocation.
 */
static size_t 
writeTheDataToMemory(void *ptr, size_t size, size_t nmemb, void *stream) {
    size_t totalSize = size * nmemb;
    WOS_MEMORY_P theMem = (WOS_MEMORY_P)stream;
   
    // If the function is called more than once, we realloc the data.
    // In principle that is a really bad idea for performance reasons
    // but since the json data this will be used for is small, this should
    // not happen enough times to matter.
    if (theMem->data == NULL) {
       theMem->data = (char *) malloc(totalSize + 1);
    } else {
       theMem->data = (char *) realloc(theMem->data, theMem->size + totalSize + 1);
    }
    if (theMem->data == NULL) {
      /* out of memory! */ 
      return(RE_OUT_OF_MEMORY);
    }
   
    // Append whatever data came in this invocation of the function
    // to the previous state of the data. Also increment the total size
    // and put on a null terminator in case this is the last invocation.
    memcpy(&(theMem->data[theMem->size]), ptr, totalSize);
    theMem->size += totalSize;
    theMem->data[theMem->size] = 0;
   
    return totalSize;
}
#endif

/** 
 * @brief This function writes the data received from the DDN unit to disk.
 *        It's used by the get operation.
 *
 *  This function conforms to the prototype required by libcurl for a 
 *  a CURLOPT_WRITEFUNCTION callback function. It writes the date returned
 *  by the curl library call to the FILE pointer defined in the stream
 *  parameter. Note that the function will often be called more than once,
 *  as libcurl defines a maximum buffer size.  This maximum can be
 *  redefined by recompiling libcurl.
 *
 * @param ptr A void ptr to the data to be written to disk.
 * @param size The size of a single item in the data: seems to always be 1
 * @param nmemb The number of items in the data.
 * @param stream A pointer to the user provided data: in this case a pointer to
 *        the FILE handle of the file to which the data will be written.
 * @return The number of bytes written.
 */
static size_t 
writeTheData(void *ptr, size_t size, size_t nmemb, void *stream) {
  size_t written = fwrite(ptr, size, nmemb, (FILE *)stream);
  return written;
}

/** 
 * @brief This function reads the data to be placed into the DDN unit
 *        from the specified file. It's used by the put operation.
 *
 *  This function conforms to the prototype required by libcurl for a 
 *  a CURLOPT_READFUNCTION callback function. It reads the data from
 *  FILE pointer defined in the stream parameter and provides the data
 *  to the libcurl POST operation. Note that the function will often be 
 *  called more than once, as libcurl defines a maximum buffer size.  
 *  This maximum can be redefined by recompiling libcurl.
 *
 * @param ptr A void ptr to the data read from the file.
 * @param size The size of single item in the data: seems to always be 1
 * @param nmemb The number of items in the data.
 * @param stream A pointer to the user provided data: in this case a pointer to
 *        the FILE handle of the file from which the data will be read.
 * @return The number of bytes read.
 */

static size_t readTheData(void *ptr, size_t size, size_t nmemb, void *stream)
{
  size_t retcode;
  // This can be optimized by recompiling libcurl as noted above.
  retcode = fread(ptr, size, nmemb, (FILE *)stream);
  return retcode;
}


/*
 * This function is called to put a file that is not yet in WOS into WOS. 
 *
 */


static int 
putNewFile(
    const char*   resource, 
    const char*   policy, 
    const char*   file, 
    WOS_HEADERS_P headerP,
    const off_t file_size) {
    CURLcode res;
    CURL *theCurl;
    time_t now;
    struct tm *theTM;
    //struct stat sourceFileInfo;
    FILE  *sourceFile;
    char theURL[WOS_RESOURCE_LENGTH + WOS_POLICY_LENGTH + 1];
    char dateHeader[WOS_DATE_LENGTH];
    char contentLengthHeader[WOS_CONTENT_HEADER_LENGTH];
    char policyHeader[strlen(WOS_POLICY_HEADER) + WOS_POLICY_LENGTH];

    // The headers
    struct curl_slist *headers = NULL;

    // Initialize lib curl
    theCurl = curl_easy_init();

    // Create the date header
    now = time(NULL);
    theTM = gmtime(&now);
    strftime(dateHeader, WOS_DATE_LENGTH, WOS_DATE_FORMAT_STRING, theTM);

    // Set the operation
    curl_easy_setopt(theCurl, CURLOPT_POST, 1);

    // Let's not dump the header or be verbose
    curl_easy_setopt(theCurl, CURLOPT_HEADER, 0);
    curl_easy_setopt(theCurl, CURLOPT_VERBOSE, 0);

    // assign the read function
    curl_easy_setopt(theCurl, CURLOPT_READFUNCTION, readTheData);

    // assign the result header function and it's user data
    curl_easy_setopt(theCurl, CURLOPT_HEADERFUNCTION, readTheHeaders);
    curl_easy_setopt(theCurl, CURLOPT_WRITEHEADER, headerP);

    sprintf(theURL, "%s%s", resource, WOS_COMMAND_PUT);

    curl_easy_setopt(theCurl, CURLOPT_URL, theURL);

    // Make the content length header
    sprintf(contentLengthHeader, "%s%ld", 
            WOS_CONTENT_LENGTH_PUT_HEADER,
            (long) (file_size));

    // Make the policy header
    sprintf(policyHeader, "%s %s", WOS_POLICY_HEADER, policy);

    // assign the data size
    curl_easy_setopt(theCurl, 
            CURLOPT_POSTFIELDSIZE_LARGE, 
            (curl_off_t) file_size);

    // Now add the headers
    headers = curl_slist_append(headers, dateHeader);
    headers = curl_slist_append(headers, contentLengthHeader);
    headers = curl_slist_append(headers, policyHeader);
    headers = curl_slist_append(headers, WOS_CONTENT_TYPE_HEADER);

    // Put the irods object reference in metadata
    //char metadata[MAX_NAME_LEN];
    //snprintf(metadata, MAX_NAME_LEN, "x-ddn-meta:\"irodsObject\":\"%s\", \"pid\": \"%d\", \"time\": \"%ld\"", file, getpid(), time(NULL));
    //headers = curl_slist_append(headers, metadata);

    // If the file size is 0, we must add dummy metadata otherwise WOS will
    // reject the put.
    if (file_size == 0) {
        std::string dummyMetadata = "x-ddn-meta:\"zeroSizeObject\":\"true\"";
        headers = curl_slist_append(headers, dummyMetadata.c_str()); 
    }

    // Stuff the headers into the request
    curl_easy_setopt(theCurl, CURLOPT_HTTPHEADER, headers);

    // Open the destination file so the handle can be passed to the
    // read function
    sourceFile = fopen(file, "rb");
    if (!sourceFile) {
        curl_easy_cleanup(theCurl);
        fclose( sourceFile );
        std::string msg( "failed to open file [" );
        msg += file;
        msg += "]";
        irods::log( ERROR( UNIX_FILE_OPEN_ERR, msg ) );
        return(UNIX_FILE_OPEN_ERR - errno);
    } else {
       curl_easy_setopt(theCurl, CURLOPT_READDATA, sourceFile);
       bool   put_done_flg = false;
       size_t retry_cnt    = 0;

       while( !put_done_flg && ( retry_cnt < RETRY_COUNT ) ) {
            res = curl_easy_perform(theCurl);
            if( res || headerP->http_status != WOS_OK || headerP->x_ddn_status != WOS_OK ) {
               // An error in libcurl
               std::stringstream msg;
               msg << "error putting the WOS object \"";
               msg << file;
               msg << "\" with curl_easy_perform status ";
               msg << res;
               msg << " (";
               msg << retry_cnt+1;
               msg << " of ";
               msg << RETRY_COUNT;
               msg << " retries ). Retry in ";
               //msg << (long) CONNECT_TIMEOUT;
               msg << "2";
               msg << " seconds.";
               irods::log( ERROR(
                           WOS_PUT_ERR,
                           msg.str() ) );

               sleep(2);
               retry_cnt++;
                
            } else {
               // libcurl return success
               put_done_flg = true;
           
           }
       }

       if( put_done_flg != true ) {
           curl_easy_cleanup(theCurl);
           std::stringstream msg;
           msg << "failed to call curl_easy_perform - ";
           msg << res; 
           irods::log( ERROR( 
                       WOS_PUT_ERR, 
                       msg.str() ) );


           fclose( sourceFile );
           return (WOS_PUT_ERR);
       }

    }
    curl_easy_cleanup(theCurl);
    fclose( sourceFile );
    return (int) res;
}

static int 
overwriteReservedFile(
    const char*   resource, 
    const char*   policy, 
    const char*   file, 
    const char*   wos_oid, 
    WOS_HEADERS_P headerP) {

    CURLcode res;
    CURL *theCurl;
    time_t now;
    struct tm *theTM;
    struct stat sourceFileInfo;
    FILE  *sourceFile;
    char theURL[WOS_RESOURCE_LENGTH + WOS_POLICY_LENGTH + 1];
    char dateHeader[WOS_DATE_LENGTH];
    char contentLengthHeader[WOS_CONTENT_HEADER_LENGTH];
    char oidHeader[WOS_FILE_LENGTH];
    char policyHeader[strlen(WOS_POLICY_HEADER) + WOS_POLICY_LENGTH];
    // The headers
    struct curl_slist *headers = NULL;

    // Initialize lib curl
    theCurl = curl_easy_init();

    // Create the date header
    now = time(NULL);
    theTM = gmtime(&now);
    strftime(dateHeader, WOS_DATE_LENGTH, WOS_DATE_FORMAT_STRING, theTM);

    // Set the operation
    curl_easy_setopt(theCurl, CURLOPT_POST, 1);

    // Let's not dump the header or be verbose
    curl_easy_setopt(theCurl, CURLOPT_HEADER, 0);
    curl_easy_setopt(theCurl, CURLOPT_VERBOSE, 0);

    // assign the read function
    curl_easy_setopt(theCurl, CURLOPT_READFUNCTION, readTheData);

    // assign the result header function and it's user data
    curl_easy_setopt(theCurl, CURLOPT_HEADERFUNCTION, readTheHeaders);
    curl_easy_setopt(theCurl, CURLOPT_WRITEHEADER, headerP);

    // We need the size of the destination file. Let's do a stat command
    if (stat(file, &sourceFileInfo)){
        rodsLog(LOG_ERROR,"stat of source file %s failed with errno %d\n", 
                file, errno);
        curl_easy_cleanup(theCurl);
        return(RE_FILE_STAT_ERROR - errno);
    }

    sprintf(theURL, "%s%s", resource, WOS_COMMAND_PUTOID);
    curl_easy_setopt(theCurl, CURLOPT_URL, theURL);

    // Make the content length header
    sprintf(contentLengthHeader, "%s%ld", 
            WOS_CONTENT_LENGTH_PUT_HEADER,
            (long) (sourceFileInfo.st_size));
   
   // Make the OID header
   sprintf(oidHeader, "%s %s", WOS_OID_HEADER, wos_oid);

    // Make the policy header
    sprintf(policyHeader, "%s %s", WOS_POLICY_HEADER, policy);

    // assign the data size
    curl_easy_setopt(theCurl, 
            CURLOPT_POSTFIELDSIZE_LARGE, 
            (curl_off_t) sourceFileInfo.st_size);

    // Now add the headers
    headers = curl_slist_append(headers, WOS_CONTENT_TYPE_HEADER);
    headers = curl_slist_append(headers, contentLengthHeader);
    headers = curl_slist_append(headers, dateHeader);
    headers = curl_slist_append(headers, oidHeader);

    // Put the irods object reference in metadata
    char metadata[MAX_NAME_LEN];
    snprintf(metadata, MAX_NAME_LEN, "x-ddn-meta:\"irodsObject\":\"%s %d %ld\"", file, getpid(), time(NULL));
    headers = curl_slist_append(headers, metadata);

    // Stuff the headers into the request
    curl_easy_setopt(theCurl, CURLOPT_HTTPHEADER, headers);

    // Open the destination file so the handle can be passed to the
    // read function
    sourceFile = fopen(file, "rb");
    if (!sourceFile) {
        curl_easy_cleanup(theCurl);
        fclose( sourceFile );
        std::string msg( "failed to open file [" );
        msg += file;
        msg += "]";
        irods::log( ERROR( UNIX_FILE_OPEN_ERR, msg ) );
        return(UNIX_FILE_OPEN_ERR - errno);
    } else {
       curl_easy_setopt(theCurl, CURLOPT_READDATA, sourceFile);
       bool   put_done_flg = false;
       size_t retry_cnt    = 0;

       while( !put_done_flg && ( retry_cnt < RETRY_COUNT ) ) {
            res = curl_easy_perform(theCurl);
            if( res || headerP->http_status != WOS_OK || headerP->x_ddn_status != WOS_OK ) {
               // An error in libcurl
               std::stringstream msg;
               msg << "error putting the WOS object \"";
               msg << file;
               msg << "\" with curl_easy_perform status ";
               msg << res;
               msg << " (";
               msg << retry_cnt+1;
               msg << " of ";
               msg << RETRY_COUNT;
               msg << " retries ). Retry in ";
               msg << (long) CONNECT_TIMEOUT;
               msg << " seconds.";
               irods::log( ERROR(
                           WOS_PUT_ERR,
                           msg.str() ) );

               retry_cnt++;
               sleep(2);    
            } else {
               // libcurl return success
               put_done_flg = true;
           
           }
       }

       if( put_done_flg != true ) {
           curl_easy_cleanup(theCurl);
           std::stringstream msg;
           msg << "failed to call curl_easy_perform - ";
           msg << res; 
           irods::log( ERROR( 
                       WOS_PUT_ERR, 
                       msg.str() ) );


           fclose( sourceFile );
           return (WOS_PUT_ERR);
       }

    }
    curl_easy_cleanup(theCurl);
    fclose( sourceFile );
    return (int) res;

}

static int 
reserveFile(
    const char*   resource, 
    const char*   policy, 
    const char*   file, 
    WOS_HEADERS_P headerP) {

    CURLcode res;
    CURL *theCurl;
    time_t now;
    struct tm *theTM;
    
    char theURL[WOS_RESOURCE_LENGTH + WOS_POLICY_LENGTH + 1];
    char dateHeader[WOS_DATE_LENGTH];
    char policyHeader[strlen(WOS_POLICY_HEADER) + WOS_POLICY_LENGTH];
    
    // The headers
    struct curl_slist *headers = NULL;

    // Initialize lib curl
    theCurl = curl_easy_init();

    // Create the date header
    now = time(NULL);
    theTM = gmtime(&now);
    strftime(dateHeader, WOS_DATE_LENGTH, WOS_DATE_FORMAT_STRING, theTM);

    // Set the operation
    curl_easy_setopt(theCurl, CURLOPT_POST, 1);

    // Let's not dump the header or be verbose
    curl_easy_setopt(theCurl, CURLOPT_HEADER, 0);
    curl_easy_setopt(theCurl, CURLOPT_VERBOSE, 0);

    // assign the read function
    curl_easy_setopt(theCurl, CURLOPT_READFUNCTION, readTheData);

    // assign the result header function and it's user data
    curl_easy_setopt(theCurl, CURLOPT_HEADERFUNCTION, readTheHeaders);
    curl_easy_setopt(theCurl, CURLOPT_WRITEHEADER, headerP);

    sprintf(theURL, "%s%s", resource, WOS_COMMAND_RESERVE);

    curl_easy_setopt(theCurl, CURLOPT_URL, theURL);

    // Make the policy header
    sprintf(policyHeader, "%s %s", WOS_POLICY_HEADER, policy);

    // Now add the headers
    headers = curl_slist_append(headers, dateHeader);
    headers = curl_slist_append(headers, policyHeader);
    headers = curl_slist_append(headers, WOS_CONTENT_TYPE_HEADER);
    headers = curl_slist_append(headers, "Content-Length: 0");

    // Stuff the headers into the request
    curl_easy_setopt(theCurl, CURLOPT_HTTPHEADER, headers);

    res = curl_easy_perform(theCurl);
    if (res ||  headerP->http_status != WOS_OK || headerP->x_ddn_status != WOS_OK ) {
        // An error in libcurl
        curl_easy_cleanup(theCurl);

        std::stringstream msg;
        msg << "failed to call curl_easy_perform - ";
        msg << res; 
        irods::log( ERROR( 
                    UNIX_FILE_OPEN_ERR, 
                    msg.str() ) );

        return (WOS_PUT_ERR);
    }

    
    long http_code = 0;
    curl_easy_getinfo (theCurl, CURLINFO_RESPONSE_CODE, &http_code);

    curl_easy_cleanup(theCurl);
    return (int) res;

}


int getL1DescIndex_for_resc_hier_and_file_path(const std::string &resc_hier, const std::string &file_path) {
    int my_idx = -1;
    for(int i = 0; i < NUM_L1_DESC; ++i) {
       if(FD_INUSE == L1desc[i].inuseFlag) { 
           dataObjInfo_t* tmp_info = L1desc[i].dataObjInfo;
           if(tmp_info && strcmp(tmp_info->rescHier, resc_hier.c_str()) == 0 && strcmp(tmp_info->filePath, file_path.c_str())) {
                   my_idx = i;
                   break;
           }
       }
    }
    return my_idx;
}

/** 
 * @brief This function registers a zero length replica on the archive
 *        resource so that the WOS id can be stored in case a failure 
 *        occurs during saving to WOS.  This allows tracking of WOS objects
 *        that could otherwise be orphaned.
 *
 * @param _ctx The plugin context 
 * @param _wos_oid A character pointer to the WOS id 
 * @return res.  An irods::error object.  Either SUCCESS() or whatever error we receive. 
 */
irods::error register_replica(irods::plugin_context& _ctx, const char *_wos_oid) {

    if(!_wos_oid) {
        return ERROR(SYS_NULL_INPUT, "null wos oid pointer");
    }

    irods::file_object_ptr file_obj = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );

    std::vector< irods::physical_object > objs = file_obj->replicas();

    // =-=-=-=-=-=-=-
    // get our resource id
    rodsLong_t resc_id = 0;
    irods::error ret = _ctx.prop_map().get<rodsLong_t>( irods::RESOURCE_ID, resc_id );
    if( !ret.ok() ) {
        return PASS( ret );
    }

    std::string resc_hier;
    ret = resc_mgr.leaf_id_to_hier(resc_id, resc_hier);
    if( !ret.ok() ) {
        return PASS( ret );
    }

    // =-=-=-=-=-=-=-
    // get the root resc of the hier
    std::string root_resc;
    irods::hierarchy_parser parser;
    parser.set_string( resc_hier );
    parser.first_resc( root_resc );

    int max_repl_num = 0;
    for (auto itr  = objs.begin();
          itr != objs.end();
          ++itr ) {
        if( itr->repl_num() > max_repl_num ) {
            max_repl_num = itr->repl_num();

        }

    } // for itr

    // =-=-=-=-=-=-=-
    // build out a dataObjInfo_t struct for use in the call
    // to rsRegDataObj.
    // At this point much of the object data will be defaulted as this
    // is not registering a real object at this time.
    dataObjInfo_t dst_data_obj;
    bzero( &dst_data_obj, sizeof( dst_data_obj ) );
    
    strncpy( dst_data_obj.objPath,       file_obj->logical_path().c_str(),             MAX_NAME_LEN );
    strncpy( dst_data_obj.rescName,      root_resc.c_str(),               NAME_LEN );
    strncpy( dst_data_obj.rescHier,      resc_hier.c_str(),               MAX_NAME_LEN );
    strncpy( dst_data_obj.dataType,      "generic",       NAME_LEN );

    // at this point we just set the size to 0 to indicate that
    // the object does not really exist in WOS.
    dst_data_obj.dataSize = 0; 
    strncpy( dst_data_obj.filePath,      _wos_oid,                  MAX_NAME_LEN );
    dst_data_obj.replNum    = max_repl_num+1;
    dst_data_obj.rescId = resc_id;
    dst_data_obj.replStatus = 0;
    dst_data_obj.dataId = file_obj->id(); 
    dst_data_obj.dataMapId = 0; 
    dst_data_obj.flags     = 0; 

    // =-=-=-=-=-=-=-
    // manufacture a src data obj
    dataObjInfo_t src_data_obj;
    memcpy( &src_data_obj, &dst_data_obj, sizeof( dst_data_obj ) );
    src_data_obj.replNum = 0;//file_obj->repl_num();
    strncpy( src_data_obj.filePath, file_obj->physical_path().c_str(),       MAX_NAME_LEN );
    strncpy( src_data_obj.rescHier, file_obj->resc_hier().c_str(),  MAX_NAME_LEN );
    // =-=-=-=-=-=-=-
    // repl to an existing copy
    regReplica_t reg_inp;
    bzero( &reg_inp, sizeof( reg_inp ) );
    reg_inp.srcDataObjInfo  = &src_data_obj;
    reg_inp.destDataObjInfo = &dst_data_obj;
    addKeyVal(&reg_inp.condInput, IN_PDMO_KW, resc_hier.c_str());
    int status = rsRegReplica( _ctx.comm(), &reg_inp );
    if( status < 0 ) {
        return ERROR( status, "failed to register data object" );
    }
 
    keyValPair_t kvp;
    memset(&kvp, 0, sizeof(kvp));

    addKeyVal(&kvp, DATA_SIZE_KW, "0");
    addKeyVal(&kvp, IN_PDMO_KW, resc_hier.c_str());

    modDataObjMeta_t mod_obj_meta;
    memset( &mod_obj_meta, 0, sizeof(mod_obj_meta));
    mod_obj_meta.regParam = &kvp;
    mod_obj_meta.dataObjInfo = &dst_data_obj;

    status = rsModDataObjMeta(_ctx.comm(), &mod_obj_meta);

    if (status < 0) {
        return ERROR( status, "failed in rsModObjMeta" );
    }

    // =-=-=-=-=-=-=-
    // we need to make a physical object and add it to the file_obj
    // so it can get picked up for the repl operation
    irods::physical_object phy_obj;
    phy_obj.resc_hier( dst_data_obj.rescHier );
    phy_obj.repl_num( dst_data_obj.replNum );
    objs.push_back( phy_obj );
    file_obj->replicas( objs );

    // =-=-=-=-=-=-=-
    // repave resc hier in file object as it is
    // what is used to determine hierarchy in
    // the compound resource
    file_obj->resc_hier( dst_data_obj.rescHier );
    file_obj->physical_path( dst_data_obj.filePath );

    // to put in getL1Desc(), also add file_path
    int my_idx = getL1DescIndex_for_resc_hier_and_file_path(resc_hier, dst_data_obj.filePath);

    if(my_idx != -1) {
        L1desc[my_idx].dataObjInfo->dataSize = 0;
    }
    else {
        return ERROR(SYS_INVALID_INPUT_PARAM, "failed to find my L1desc index");
    }


    return SUCCESS();

} // register_replica

/** 
 * @brief This function is the high level function that adds a data file
 *        to the DDN storage using the WOS interface.
 *
 *  This function uses the libcurl API to POST the specified file
 *  to the DDN using the WOS interface. See http://curl.haxx.se/libcurl/
 *  for information about libcurl.
 *
 * @param resource A character pointer to the resource for this request.
 * @param policy A character pointer to the policy for this request.
 * @param file A character pointer to the file for this request.
 * @param headerP A pointer to WOS_HEADERS structure that will be filled in.
 * @return res.  The return code from curl_easy_perform.
 */
static int putTheFile(
    const char*   resource, 
    const char*   policy, 
    const char*   file, 
    const char*   prev_oid, 
    irods::plugin_context& _ctx,
    WOS_HEADERS_P headerP) {

    const off_t single_buff_sz = irods::get_advanced_setting<const int>(irods::CFG_MAX_SIZE_FOR_SINGLE_BUFFER) * 1024 * 1024;


    WOS_HEADERS theHeaders;
    memset( &theHeaders, 0, sizeof( theHeaders ) );

    std::string wos_oid;
    bool file_on_wos = false;

    // =-=-=-=-=-=-=-
    // We need the size of the destination file. Let's do a stat command
    struct stat sourceFileInfo;
    memset( &sourceFileInfo, 0, sizeof( sourceFileInfo ) );
    if (stat(file, &sourceFileInfo)){
        rodsLog(LOG_ERROR,"stat of source file %s failed with errno %d\n", 
                file, errno);
        return(RE_FILE_STAT_ERROR - errno);
    }


    // =-=-=-=-=-=-=-
    // stat the WOS file, if it exists on the wos system we need to check 
    // the size.  If it is non-zero, register first to get OID and then overwrite.
    // If it is zero then just put the file. 
    int status = 1;
    std::string prev_oid_str( prev_oid );

    // only query if we have a valid oid ( no path separators )
    if (std::string::npos == prev_oid_str.find( "/" ) ) {
        status = getTheFileStatus( 
                     resource, 
                     prev_oid,
                     &theHeaders);
    }

    // returns non-zero on error.
    if( !status ) {
        wos_oid      = theHeaders.x_ddn_oid;
        file_on_wos  = true;
    }


    if( file_on_wos && WOS_UNUSED_RESERVATION == theHeaders.x_ddn_status ) {
        status = overwriteReservedFile(
                     resource,
                     policy,
                     file,
                     wos_oid.c_str(),
                     headerP );
    }
    else if (sourceFileInfo.st_size < single_buff_sz) {

        status = putNewFile(
                     resource,
                     policy,
                     file,
                     headerP,
                     sourceFileInfo.st_size );
    } else {
        status = reserveFile(
                     resource,
                     policy,
                     file,
                     headerP );
        if(status) {
            rodsLog(LOG_ERROR, "putTheFile - reserveFile failed [%d]", status);
            return status;
        }


        irods::error get_ret = register_replica(_ctx, headerP->x_ddn_oid);
        if (!get_ret.ok()) {
            irods::log(get_ret);
        }

        rodsLog(LOG_DEBUG, "received wos oid - %s\n", headerP->x_ddn_oid);

        status = overwriteReservedFile(
                     resource,
                     policy,
                     file,
                     headerP->x_ddn_oid,
                     headerP );

        rodsLog(LOG_DEBUG, "finished writing to wos oid - %s", headerP->x_ddn_oid);

    }

    return status;
}

/** 
 * @brief This function is the high level function that retrieves a data file
 *        from the DDN storage using the WOS interface.
 *
 *  This function uses the libcurl API to GET the specified file
 *  to the DDN using the WOS interface. See http://curl.haxx.se/libcurl/
 *  for information about libcurl.
 *
 * @param resource A character pointer to the resource for this request.
 * @param file A character pointer to the file to retrieve for this request.
 *             This will be a DDN OID.
 * @param destination A character pointer to the destination for this request.
 *        This is a file.
 * @param mode The file mode to be used when creating the file.  This comes
 *        from the users original file and is stored in the icat.
 * @param headerP A pointer to WOS_HEADERS structure that will be filled in.
 * @return res.  The return code from curl_easy_perform.
 */

static int getTheFile(
    const char *resource, 
    const char *file, 
    const char *destination, 
    int mode,
    WOS_HEADERS_P headerP) {
    CURLcode res;
    CURL *theCurl;
    time_t now;
    struct tm *theTM;
    FILE  *destFile;
    int    destFd;
    // Initialize lib curl
    theCurl = curl_easy_init();

    // The extra byte is for the '/'
    char theURL[WOS_RESOURCE_LENGTH + WOS_POLICY_LENGTH + 1];
    char dateHeader[WOS_DATE_LENGTH];

    // The headers
    struct curl_slist *headers = NULL;

    // construct the url from the resource and the file name
    sprintf(theURL, "%s/objects/%s", resource, file);
    curl_easy_setopt(theCurl, CURLOPT_URL, theURL);

    // Create the date header
    now = time(NULL);
    theTM = gmtime(&now);
    strftime(dateHeader, WOS_DATE_LENGTH, WOS_DATE_FORMAT_STRING, theTM);

    // Set the request header
    curl_easy_setopt(theCurl, CURLOPT_HTTPGET, 1);

    // Let's not dump the header or be verbose
    curl_easy_setopt(theCurl, CURLOPT_HEADER, 0);
    curl_easy_setopt(theCurl, CURLOPT_VERBOSE, 0);

    // Now add some headers
    headers = curl_slist_append(headers, WOS_CONTENT_TYPE_HEADER);
    headers = curl_slist_append(headers, WOS_CONTENT_LENGTH_HEADER);
    headers = curl_slist_append(headers, dateHeader);

    // Get rid of the accept header
    headers = curl_slist_append(headers, "Accept:");

    // Stuff the headers into the request
    curl_easy_setopt(theCurl, CURLOPT_HTTPHEADER, headers);

    // assign the write funcion
    curl_easy_setopt(theCurl, CURLOPT_WRITEFUNCTION, writeTheData);

    // assign the result header function and it's user data
    curl_easy_setopt(theCurl, CURLOPT_HEADERFUNCTION, readTheHeaders);
    curl_easy_setopt(theCurl, CURLOPT_WRITEHEADER, headerP);

    // Set connection timeout (seconds)
    curl_easy_setopt(theCurl, CURLOPT_CONNECTTIMEOUT, (long) CONNECT_TIMEOUT);

    // Open the destination file using open so we can use the user mode.
    // Then convert the file index into a descriptor for use of the curl
    // library
    destFd = open(destination, O_WRONLY | O_CREAT | O_TRUNC, mode);
    if (destFd < 0) {
        curl_easy_cleanup(theCurl);
        return(UNIX_FILE_OPEN_ERR - errno);
    } else {
        destFile = fdopen(destFd, "wb");
        if (!destFile) {
            // Couldn't convert index to descriptor.  Seems unlikely ...
            curl_easy_cleanup(theCurl);
            fclose( destFile );
            return(UNIX_FILE_OPEN_ERR - errno);
        } else {
            curl_easy_setopt(theCurl, CURLOPT_FILE, destFile);
            bool   get_done_flg = false;
            size_t retry_cnt    = 0;

            while( !get_done_flg && ( retry_cnt < RETRY_COUNT ) ) {
                res = curl_easy_perform(theCurl);
                if ( res || headerP->http_status != WOS_OK || headerP->x_ddn_status != WOS_OK ) {
                    // an error in lib curl
                    std::stringstream msg;
                    msg << "error getting the WOS object \"";
                    msg << file;
                    msg << "\" with curl_easy_perform status ";
                    msg << res;
                    msg << " (";
                    msg << retry_cnt+1;
                    msg << " of ";
                    msg << RETRY_COUNT;
                    msg << " retries ). Retry in ";
                    msg << (long) CONNECT_TIMEOUT;
                    msg << " seconds.";
                    irods::log( ERROR(
                                WOS_GET_ERR,
                                msg.str() ) );

                    retry_cnt++;

                } else {
                    // libcurl return success
                    get_done_flg = true;

                }

            } // while

            if( !get_done_flg ) {
                fclose( destFile );
                unlink(destination);
                curl_easy_cleanup(theCurl);
                std::stringstream msg;
                msg << "failed to call curl_easy_perform - ";
                msg << res; 
                irods::log( 
                    ERROR( 
                        WOS_GET_ERR, 
                        msg.str() ) );
                return(WOS_GET_ERR);

            } // if

        } // else 

    } // else

    if (headerP->x_ddn_status == WOS_OBJ_NOT_FOUND) {
        // The file was not found but because we already opened it
        // there will now be a zero length file. Let's remove it
        unlink(destination);
    }
    curl_easy_cleanup(theCurl);
    fclose(destFile);
    return res;

}

/** 
 * @brief This function is the high level function that retrieves the 
 *        status of a data file from the DDN storage using the WOS interface.
 *
 *  This function uses the libcurl API to HEAD the specified file
 *  from the DDN using the WOS interface. See http://curl.haxx.se/libcurl/
 *  for information about libcurl.
 *
 * @param resource A character pointer to the resource for this request.
 * @param file A character pointer to the file to retrieve for this request.
 * @param headerP A pointer to WOS_HEADERS structure that will be filled in.
 * @return res.  The return code from curl_easy_perform.
 */

static int 
getTheFileStatus (const char *resource, const char *file, WOS_HEADERS_P headerP) {

   CURLcode res;
   CURL *theCurl;
   time_t now;
   struct tm *theTM;
   // Initialize lib curl
   theCurl = curl_easy_init();

   // The extra byte is for the '/'
   char theURL[WOS_RESOURCE_LENGTH + WOS_POLICY_LENGTH + 1];
   char dateHeader[WOS_DATE_LENGTH];
 
   // The headers
   struct curl_slist *headers = NULL;

   // construct the url from the resource and the file name
   sprintf(theURL, "%s/objects/%s", resource, file);
   curl_easy_setopt(theCurl, CURLOPT_URL, theURL);

   // Create the date header
   now = time(NULL);
   theTM = gmtime(&now);
   strftime(dateHeader, WOS_DATE_LENGTH, WOS_DATE_FORMAT_STRING, theTM);

   // Set the request header
   curl_easy_setopt(theCurl, CURLOPT_NOBODY, 1);

   // Let's not dump the header or be verbose
   curl_easy_setopt(theCurl, CURLOPT_HEADER, 0);
   curl_easy_setopt(theCurl, CURLOPT_VERBOSE, 0);

   // Now add some headers
   headers = curl_slist_append(headers, WOS_CONTENT_TYPE_HEADER);
   headers = curl_slist_append(headers, WOS_CONTENT_LENGTH_HEADER);
   headers = curl_slist_append(headers, dateHeader);

   // Get rid of the accept header
   headers = curl_slist_append(headers, "Accept:");
   
   // Stuff the headers into the request
   curl_easy_setopt(theCurl, CURLOPT_HTTPHEADER, headers);

   // assign the result header function and it's user data
   curl_easy_setopt(theCurl, CURLOPT_HEADERFUNCTION, readTheHeaders);
   curl_easy_setopt(theCurl, CURLOPT_WRITEHEADER, headerP);
   
   // Call the operation, added retry logic as sometimes we get an x_ddn_status of 214 - TemporarilyNotSupported
   size_t retry_cnt = 0;
   while( retry_cnt < RETRY_COUNT ) {
       res = curl_easy_perform(theCurl);

        // retry if we get WOS_TEMPORARILY_NOT_SUPPORTED
        if ( headerP->x_ddn_status == WOS_TEMPORARILY_NOT_SUPPORTED) {
            std::stringstream msg;
            msg << "error getting the file status\"";
            msg << file;
            msg << "\" with curl_easy_perform status ";
            msg << res; 
            msg << " (";
            msg << retry_cnt+1;
            msg << " of "; 
            msg << RETRY_COUNT;
            msg << " retries ). Retry in two seconds.";
            irods::log( ERROR( WOS_GET_ERR, msg.str() ) );

            retry_cnt++;
            sleep(2);

       } else if (res || headerP->http_status != WOS_OK || headerP->x_ddn_status != WOS_OK) {
           curl_easy_cleanup(theCurl);
           return(WOS_GET_ERR);
       } else {
           // all is good 
           curl_easy_cleanup(theCurl);
           return (int) res;
       }
   }

   curl_easy_cleanup(theCurl);
   return (WOS_GET_ERR);

}


/** 
 * @brief This function is the high level function that deletes a data file
 *        from DDN storage using the WOS interface.
 *
 *  This function uses the libcurl API to POST the specified file deletion
 *  to the DDN using the WOS interface. See http://curl.haxx.se/libcurl/
 *  for information about libcurl.
 *
 * @param resource A character pointer to the resource for this request.
 * @param file A character pointer to the file for this request.
 * @param headerP A pointer to WOS_HEADERS structure that will be filled in.
 * @return res.  The return code from curl_easy_perform.
 */
static 
int deleteTheFile (const char *resource, const char *file, WOS_HEADERS_P headerP) {

   // perform a stat to see if the file is really on WOS
   int status = getTheFileStatus(resource, file, headerP);
    
   // if the object does not exist (status is non-zero) then
   // just return CURLE_OK
   if (status) {
       rodsLog(LOG_DEBUG6, "Called deleteTheFile on OID %s but it does not appear to be in WOS.  Returning success.", file);
       return CURLE_OK;
   } 

   CURLcode res;
   CURL *theCurl;
   time_t now;
   struct tm *theTM;
   char theURL[WOS_RESOURCE_LENGTH + WOS_POLICY_LENGTH + 1];
   char dateHeader[WOS_DATE_LENGTH];
   char contentLengthHeader[WOS_CONTENT_HEADER_LENGTH];
   char oidHeader[WOS_FILE_LENGTH];
   bool put_done_flg = false;
   size_t retry_cnt    = 0;
   // Initialize lib curl
   theCurl = curl_easy_init();
 
   // The headers
   struct curl_slist *headers = NULL;

   // Create the date header
   now = time(NULL);
   theTM = gmtime(&now);
   strftime(dateHeader, WOS_DATE_LENGTH, WOS_DATE_FORMAT_STRING, theTM);

   // Set the operation
   curl_easy_setopt(theCurl, CURLOPT_POST, 1);
   
   // construct the url from the resource and the put command
   sprintf(theURL, "%s%s", resource, WOS_COMMAND_DELETE);
   curl_easy_setopt(theCurl, CURLOPT_URL, theURL);

   // Let's not dump the header or be verbose
   curl_easy_setopt(theCurl, CURLOPT_HEADER, 0);
   curl_easy_setopt(theCurl, CURLOPT_VERBOSE, 0);

   // assign the result header function and it's user data
   curl_easy_setopt(theCurl, CURLOPT_HEADERFUNCTION, readTheHeaders);
   curl_easy_setopt(theCurl, CURLOPT_WRITEHEADER, headerP);
   
   // Set connection timeout (seconds)
   curl_easy_setopt(theCurl, CURLOPT_CONNECTTIMEOUT, (long) CONNECT_TIMEOUT);

   // Make the content length header
   sprintf(contentLengthHeader, "%s%d", WOS_CONTENT_LENGTH_PUT_HEADER, 0);

   // Make the OID header
   sprintf(oidHeader, "%s %s", WOS_OID_HEADER, file);

   
   // Now add the headers
   headers = curl_slist_append(headers, dateHeader);
   headers = curl_slist_append(headers, contentLengthHeader);
   headers = curl_slist_append(headers, oidHeader);
   headers = curl_slist_append(headers, WOS_CONTENT_TYPE_HEADER);
  
   // Stuff the headers into the request
   curl_easy_setopt(theCurl, CURLOPT_HTTPHEADER, headers);
   while( !put_done_flg && ( retry_cnt < RETRY_COUNT ) ) {

      res = curl_easy_perform(theCurl);
      if ( res || headerP->http_status != WOS_OK || headerP->x_ddn_status != WOS_OK ) {
         // An error in libcurl
         std::stringstream msg;
         msg << "error deleting the WOS object \"";
         msg << file;
         msg << "\" with curl_easy_perform status ";
         msg << res;
         msg << " (";
         msg << retry_cnt+1;
         msg << " of ";
         msg << RETRY_COUNT;
         msg << " retries ). Retry in ";
         msg << "2";
         //msg << (long) CONNECT_TIMEOUT;
         msg << " seconds.";
         irods::log( ERROR(
                     WOS_UNLINK_ERR,
                     msg.str() ) );

         retry_cnt++;

         sleep(2);

      } else {
         // libcurl return success
         put_done_flg = true;

      }

   } // while

   if( put_done_flg != true ) {
       curl_easy_cleanup(theCurl);
       std::stringstream msg;
       msg << "failed to call curl_easy_perform - ";
       msg << res;
       irods::log( ERROR(
                   WOS_UNLINK_ERR,
                   msg.str() ) );
       return (WOS_UNLINK_ERR);
   }

   curl_easy_cleanup(theCurl);
   return (int) res;

}

#ifdef USING_JSON
/**
 * Important note about the following code. It won't work in the community code
 * and it's probably not going to.  It hasn't been removed because
 * the there is no other way to provide this functionality in the current
 * DDN rest interface and because it may work just fine in e-irods using
 * the context strings. DDN is, in theory, going to update the rest
 * interface to provide a stat operation.  When that happens, we'll add
 * a curl call for that interface and delete this code.  Until then, it
 * makes sense to leave this here. Making this json version of the code
 * work correctly would require enhancing the community iRODS resource code to store
 * and provide an admin user, password and url.
 */
/** 
 * @brief This function processes the json returned by the stats interface
 * into the a convenient structure. 
 *
 * The parsing is done using json-c (http://oss.metaparadigm.com/json-c/). On
 * Ubuntu, you get this by executing sudo apt-get install libjson0-dev. 
 * On CentOS use sudo yum install json-c-devel
 * An example usage of this library is at 
 * http://coolaj86.info/articles/json-c-example.html 
 *
 * @param statP The structure that will contain the processed json.
 * @param jsonP A character string with the json in it.
 * @return int  Either JSON_OK or JSON_ERROR
 */
int processTheStatJSON(char *jsonP, WOS_STATISTICS_P statP) {
   struct json_object *theObjectP;
   struct json_object *tmpObjectP;

   // Do the parse.
   theObjectP = json_tokener_parse(jsonP);
   if (is_error(theObjectP)) {
      statP->data = jsonP; // Error handling
      return (WOS_GET_ERR);
   }

   // For each value we care about, we get the object, then the value
   // from the object
   tmpObjectP = json_object_object_get(theObjectP, "totalNodes");
   statP->totalNodes = json_object_get_int(tmpObjectP);

   tmpObjectP = json_object_object_get(theObjectP, "activeNodes");
   statP->activeNodes = json_object_get_int(tmpObjectP);

   tmpObjectP = json_object_object_get(theObjectP, "disconnected");
   statP->disconnected = json_object_get_int(tmpObjectP);

   tmpObjectP = json_object_object_get(theObjectP, "clients");
   statP->clients = json_object_get_int(tmpObjectP);

   tmpObjectP = json_object_object_get(theObjectP, "objectCount");
   statP->objectCount = json_object_get_int(tmpObjectP);

   tmpObjectP = json_object_object_get(theObjectP, "rawObjectCount");
   statP->rawObjectCount = json_object_get_int(tmpObjectP);

   tmpObjectP = json_object_object_get(theObjectP, "usableCapacity");
   statP->usableCapacity = json_object_get_double(tmpObjectP);

   tmpObjectP = json_object_object_get(theObjectP, "capacityUsed");
   statP->capacityUsed = json_object_get_double(tmpObjectP);
   rodsLog(LOG_DEBUG3, "\tObjectCount:        %d\n\tRaw Object Count:      %d\n", 
          statP->objectCount, statP->rawObjectCount);
   rodsLog(LOG_DEBUG3, "\tCapacity used:      %f Gb\n\tCapacity available: %f GB\n", 
          statP->capacityUsed, statP->usableCapacity);
   return (JSON_OK);
}

/**
 * Important note about the following code. It's not currently used
 * and it's probably not going to be.  It hasn't been removed because
 * the there is no other way to provide this functionality in the current
 * DDN rest interface. DDN is, in theory, going to update the rest
 * interface to provide a stat operation.  When that happens, we'll add
 * a curl call for that interface and delete this code.  Until then, it
 * makes sense to leave this here. Making this json version of the code
 * work correctly would require enhancing the iRODS resource code to store
 * and provide an admin user, password and url.
 */
/** 
 * @brief This function is the high level function that retrieves data from
 *        the DDN using the admin interface.
 *
 *  This function uses the libcurl API to get the specified data
 *  from the DDN using the admin interface. See http://curl.haxx.se/libcurl/
 *  for information about libcurl. The data ends up in memory, where we
 *  use a json parser to demarshal into a structure.
 *
 * @param resource A character pointer to the resource for this request.
 * @param user A character pointer to the name of the user for this
 *             request.
 * @param password A character pointer to the password of the user for this
 *             request.
 * @param statsP A pointer to the stats structure used to return the JSON
 *               data on a parse error.
 * @return res.  The return code from curl_easy_perform.
 */


static int 
getTheManagementData( 
    const char *resource,  
    const char *user,  
    const char *password,
    WOS_STATISTICS_P statsP) {

    CURLcode   res;
    CURL *theCurl;
    WOS_MEMORY theData;
    char       auth[(WOS_AUTH_LENGTH * 2) + 1];
    // Initialize lib curl
    theCurl = curl_easy_init();

    // Init the memory struct
    theData.data = NULL;
    theData.size = 0;

    // Copy the resource into the URL
    curl_easy_setopt(theCurl, CURLOPT_URL, resource);

    // Let's not dump the header or be verbose
    curl_easy_setopt(theCurl, CURLOPT_HEADER, 0);
    curl_easy_setopt(theCurl, CURLOPT_VERBOSE, 0);

    // assign the write function and the pointer
    curl_easy_setopt(theCurl, CURLOPT_WRITEFUNCTION, writeTheDataToMemory);
    curl_easy_setopt(theCurl, CURLOPT_WRITEDATA, &theData);

    // Add the user name and password
    sprintf(auth, "%s:%s", user, password);
    curl_easy_setopt(theCurl, CURLOPT_USERPWD, auth);
    curl_easy_setopt(theCurl, CURLOPT_HTTPAUTH, (long) CURLAUTH_ANY);
  
    res = curl_easy_perform(theCurl);
    if (res) {
       // libcurl error
       return(WOS_GET_ERR);
    }

    res = (CURLcode) processTheStatJSON(theData.data, statsP);
   
    curl_easy_cleanup(theCurl);

    return((int) res);
}
#else
static int 
getTheManagementData( 
    const char *resource,  
    const char *user,  
    const char *password,
    WOS_STATISTICS_P statsP) {
    return 0;
}
#endif

// =-=-=-=-=-=-=-
/// @brief Checks the basic operation parameters and updates the physical path in the file object
irods::error wosCheckParams(irods::plugin_context& _ctx ) {

    irods::error ret;

    // =-=-=-=-=-=-=-
    // check incoming parameters
    // =-=-=-=-=-=-=-
    // verify that the resc context is valid 
    ret = _ctx.valid();
    return ( ASSERT_PASS(ret, "wosCheckParams - resource context is invalid"));

} // wosCheckParams
    
    // =-=-=-=-=-=-=-
    // interface for file registration
    irods::error wosRegisteredPlugin( irods::plugin_context& _ctx) {

        return ERROR( SYS_NOT_SUPPORTED, "wosRegisteredPlugin" );
    } // wosRegisteredPlugin

    // =-=-=-=-=-=-=-
    // interface for file unregistration
    irods::error wosUnregisteredPlugin( irods::plugin_context& _ctx) {

        return ERROR( SYS_NOT_SUPPORTED, "wosUnregisteredPlugin" );
    } // wosUnregisteredPlugin

    // =-=-=-=-=-=-=-
    // interface for file modification
    irods::error wosModifiedPlugin( irods::plugin_context& _ctx) {

        return ERROR( SYS_NOT_SUPPORTED, "wosModifiedPlugin" );
    } // wosModifiedPlugin
    
    // =-=-=-=-=-=-=-
    // interface for POSIX create
    irods::error wosFileCreatePlugin( irods::plugin_context& _ctx) {

        return ERROR( SYS_NOT_SUPPORTED, "wosFileCreatePlugin" );
    } // wosFileCreatePlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX Open
    irods::error wosFileOpenPlugin( irods::plugin_context& _ctx) {

        return ERROR( SYS_NOT_SUPPORTED, "wosFileOpenPlugin" );
    } // wosFileOpenPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX Read
    irods::error wosFileReadPlugin( irods::plugin_context& _ctx,
                                      void*               _buf, 
                                      int                 _len ) {
                                      
        return ERROR( SYS_NOT_SUPPORTED, "wosFileReadPlugin" );

    } // wosFileReadPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX Write
    irods::error wosFileWritePlugin( irods::plugin_context& _ctx,
                                       void*               _buf, 
                                       int                 _len ) {
        return ERROR( SYS_NOT_SUPPORTED, "wosFileWritePlugin" );

    } // wosFileWritePlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX Close
    irods::error wosFileClosePlugin(  irods::plugin_context& _ctx ) {

        return ERROR( SYS_NOT_SUPPORTED, "wosFileClosePlugin" );
        
    } // wosFileClosePlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX Unlink
    irods::error wosFileUnlinkPlugin( irods::plugin_context& _ctx ) {
        int status;
        const char *wos_host;
        WOS_HEADERS theHeaders;
        memset( &theHeaders, 0, sizeof( theHeaders ) );

        irods::error prop_ret;
        std::string my_host;
        irods::error result = SUCCESS();

        // =-=-=-=-=-=-=-
        // check incoming parameters
        irods::error ret = wosCheckParams( _ctx );
        if((result = ASSERT_PASS(ret, "Invalid parameters or physical path.")).ok()) {
        
           irods::plugin_property_map& prop_map = _ctx.prop_map();
   
           prop_ret = prop_map.get< std::string >( WOS_HOST_KEY, my_host );
           if((result = ASSERT_PASS(prop_ret, "- prop_map has no wos_host.")).ok()) {
              wos_host = my_host.c_str();
      
              // =-=-=-=-=-=-=-
              // get ref to data object
              irods::data_object_ptr object = boost::dynamic_pointer_cast< irods::data_object >( _ctx.fco() );
      
              status = 
               deleteTheFile(wos_host, object->physical_path().c_str(), &theHeaders);

              // error handling
              if( status < 0 ) {
                  result =  ERROR( WOS_UNLINK_ERR, "wosFileUnlinkPlugin - error in deleteTheFile");
              }
           }
        }

        return result;
    } // wosFileUnlinkPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX Stat
    irods::error wosFileStatPlugin(
        irods::plugin_context& _ctx,
        struct stat*           _statbuf ) { 
        rodsLong_t len;
        int status = 0;
        const char *wos_host;
        WOS_HEADERS theHeaders;
        memset( &theHeaders, 0, sizeof( theHeaders ) );
        irods::error prop_ret;
        std::string my_host;
        irods::error result = SUCCESS();

        // =-=-=-=-=-=-=-
        // check incoming parameters
        irods::error ret = wosCheckParams( _ctx );
        if((result = ASSERT_PASS(ret, "Invalid parameters or physical path.")).ok()) {

           irods::plugin_property_map& prop_map = _ctx.prop_map();
           prop_ret = prop_map.get< std::string >( WOS_HOST_KEY, my_host );
           if((result = ASSERT_PASS(prop_ret, " - prop_map has no wos_host")).ok()) {
              wos_host = my_host.c_str();
      
              // =-=-=-=-=-=-=-
              // get ref to data object
              irods::data_object_ptr object = boost::dynamic_pointer_cast< irods::data_object >( _ctx.fco() );

             // Call the WOS function
              status = getTheFileStatus(wos_host, object->physical_path().c_str(), &theHeaders);
      
              // returns non-zero on error.
              if (!status) {
                 // This is the info we want.
                 len = theHeaders.x_ddn_length;
             
                 // Fill in the rest of the struct.  Note that this code is carried over
                 // from the original code.
                 if (len >= 0 && theHeaders.x_ddn_status == 0 ) {
                    _statbuf->st_mode = S_IFREG;
                    _statbuf->st_nlink = 1;
                    _statbuf->st_uid = getuid ();
                    _statbuf->st_gid = getgid ();
                    _statbuf->st_atime = _statbuf->st_mtime = _statbuf->st_ctime = time(0);
                    _statbuf->st_size = len;
                 }
              } else {
                result =  ERROR( theHeaders.x_ddn_status, "wosFileStatPlugin - error in getTheFileStatus");
              } 
           }
        }
        return result;

    } // wosFileStatPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX lseek
    irods::error wosFileLseekPlugin(  irods::plugin_context& _ctx, 
                                       size_t              _offset, 
                                       int                 _whence ) {

        return ERROR( SYS_NOT_SUPPORTED, "wosFileLseekPlugin" );
                                       
    } // wosFileLseekPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX fsync
    irods::error wosFileFsyncPlugin(  irods::plugin_context& _ctx ) {

        return ERROR( SYS_NOT_SUPPORTED, "wosFileFsyncPlugin" );

    } // wosFileFsyncPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX mkdir
    irods::error wosFileMkdirPlugin(  irods::plugin_context& _ctx ) {

        return ERROR( SYS_NOT_SUPPORTED, "wosFileMkdirPlugin" );

    } // wosFileMkdirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX mkdir
    irods::error wosFileRmdirPlugin(  irods::plugin_context& _ctx ) {

        return ERROR( SYS_NOT_SUPPORTED, "wosFileRmdirPlugin" );
    } // wosFileRmdirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX opendir
    irods::error wosFileOpendirPlugin( irods::plugin_context& _ctx ) {

        return ERROR( SYS_NOT_SUPPORTED, "wosFileOpendirPlugin" );
    } // wosFileOpendirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX closedir
    irods::error wosFileClosedirPlugin( irods::plugin_context& _ctx) {

        return ERROR( SYS_NOT_SUPPORTED, "wosFileClosedirPlugin" );
    } // wosFileClosedirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX readdir
    irods::error wosFileReaddirPlugin( irods::plugin_context& _ctx,
                                        struct rodsDirent**     _dirent_ptr ) {

        return ERROR( SYS_NOT_SUPPORTED, "wosFileReaddirPlugin" );
    } // wosFileReaddirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX readdir
    irods::error wosFileRenamePlugin( irods::plugin_context& _ctx,
                                       const char*         _new_file_name ) {

        return ERROR( SYS_NOT_SUPPORTED, "wosFileRenamePlugin" );
    } // wosFileRenamePlugin

    
    // interface to determine free space on a device given a path
    irods::error wosFileGetFsFreeSpacePlugin(
        irods::plugin_context& _ctx ){

        irods::error prop_ret;
        std::string my_admin;
        std::string my_user;
        std::string my_password;
        int status;
        const char *wos_admin;
        const char *wos_user;
        const char *wos_password;
        WOS_STATISTICS theStats;
        rodsLong_t spaceInBytes;
        irods::error result = SUCCESS();

        // =-=-=-=-=-=-=-
        // check incoming parameters
        irods::error ret = wosCheckParams( _ctx );
        if(!(result = ASSERT_PASS(ret, "Invalid parameters or physical path.")).ok()) {
            return result;
        } 

        irods::plugin_property_map& prop_map = _ctx.prop_map();
        prop_ret = prop_map.get< std::string >( "wos_admin_URL", my_admin );
        if(!(result = ASSERT_PASS(prop_ret, " - prop_map has no wos_admin_url")).ok()) {
            return result;
        }
        wos_admin = my_admin.c_str();

        prop_ret = prop_map.get< std::string >( "wos_admin_user", my_user );
        if(!(result = ASSERT_PASS(prop_ret, " - prop_map has no wos_admin_user")).ok()) {
            return result;
        }
        wos_user = my_user.c_str();

        prop_ret = prop_map.get< std::string >( "wos_admin_password", my_password );
        if(!(result = ASSERT_PASS(prop_ret, " - prop_map has no wos_admin_password")).ok()) {
            return result;
        }
        wos_password = my_password.c_str();

        status = getTheManagementData(wos_admin, wos_user, wos_password, &theStats);

        // returns non-zero on error.
        if (status) {
            result =  ERROR( status, 
                    "wosFileGetFsFreeSpacePlugin - error in getTheManagementData");
            return result;
        }

        // Units are in Gb 
        spaceInBytes = theStats.usableCapacity - theStats.capacityUsed;
        spaceInBytes *= 1073741824;
        result.code(spaceInBytes);

        return result;

    } // wosFileGetFsFreeSpacePlugin


    // =-=-=-=-=-=-=-
    // wosStageToCache - This routine is for testing the TEST_STAGE_FILE_TYPE.
    // Just copy the file from filename to cacheFilename. optionalInfo info
    // is not used.
    irods::error wosStageToCachePlugin(
            irods::plugin_context& _ctx,
            const char*            _cache_file_name ) {
        int status;

        const char *wos_host = nullptr;

        irods::error prop_ret;
        std::string my_host;
        std::ostringstream out_stream;
        irods::error result = SUCCESS();


        WOS_HEADERS theHeaders;
        memset( &theHeaders, 0, sizeof( theHeaders ) );

        // check incoming parameters
        irods::error ret = wosCheckParams( _ctx );
        if((result = ASSERT_PASS(ret, "Invalid parameters or physical path.")).ok()) {
            irods::plugin_property_map&  prop_map = _ctx.prop_map();

            prop_ret = prop_map.get< std::string >( WOS_HOST_KEY, my_host );
            if((result = ASSERT_PASS(prop_ret, "- prop_map has no wos_host.")).ok()) { 
                wos_host = my_host.c_str();

                irods::file_object_ptr file_obj = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
                // The old code allows user to set a mode.  We should now be doing this.
                status = getTheFile(wos_host, 
                        file_obj->physical_path().c_str(), 
                        _cache_file_name, 
                        file_obj->mode(), 
                        &theHeaders);
                if(status) {
                    result =  ERROR( status, "wosStageToCachePlugin - error in getTheFile");
                }
            }
        }

        return result;
    } // wosStageToCachePlugin

    irods::error unlink_for_overwrite(
        const char*            _wos_host,
        irods::plugin_context& _ctx) {

        irods::file_object_ptr fobj = boost::dynamic_pointer_cast<
            irods::file_object>(
                    _ctx.fco());
        std::string resc_name;
        irods::error ret = _ctx.prop_map().get<std::string>(
                irods::RESOURCE_NAME,
                resc_name );
        if( !ret.ok() ) {
            return PASS( ret );
        }

        irods::hierarchy_parser hp;
        hp.set_string(fobj->resc_hier());
        if(hp.resc_in_hier(resc_name)) {
            WOS_HEADERS theHeaders;
            memset( &theHeaders, 0, sizeof( theHeaders ) );

            std::string vault_path;
            _ctx.prop_map().get<std::string>(irods::RESOURCE_PATH, vault_path);

            // only delete the file if we have an OID and not a vault_path
            if (fobj->physical_path().find(vault_path) == std::string::npos) {
                deleteTheFile(
                    _wos_host,
                    fobj->physical_path().c_str(),
                    &theHeaders);
            } 

        }

        return SUCCESS();

    } // unlink_for_overwrite

    // =-=-=-=-=-=-=-
    // wosSyncToArch - This routine is for testing the TEST_STAGE_FILE_TYPE.
    // Just copy the file from cacheFilename to filename. optionalInfo info
    // is not used.
    irods::error wosSyncToArchPlugin( 
        irods::plugin_context& _ctx,
        const char*            _cache_file_name ) {
        int status;
        const char *wos_host = nullptr;
        const char *wos_policy = nullptr;

        WOS_HEADERS theHeaders;
        memset( &theHeaders, 0, sizeof( theHeaders ) );
        WOS_HEADERS deleteHeaders;
        memset( &deleteHeaders, 0, sizeof( deleteHeaders ) );

        irods::error prop_ret;
        std::string my_host;
        std::string my_policy;
        std::ostringstream out_stream;
        irods::error result = SUCCESS();

        // check incoming parameters
        irods::error ret = wosCheckParams( _ctx );
        if((result = ASSERT_PASS(ret, "Invalid parameters or physical path.")).ok()) {
           irods::plugin_property_map& prop_map = _ctx.prop_map();
           prop_ret = prop_map.get< std::string >( WOS_HOST_KEY, my_host );

           if((result = ASSERT_PASS(prop_ret, "- prop_map has no wos_host.")).ok()) {
              wos_host = my_host.c_str();

              irods::error err = unlink_for_overwrite(wos_host, _ctx);
              if(!err.ok()) {
                  irods::log(err);
              }

              prop_ret = prop_map.get< std::string >( WOS_POLICY_KEY, my_policy );
              if((result = ASSERT_PASS(prop_ret, "- prop_map has no wos_policy.")).ok()) {
                 wos_policy = my_policy.c_str();
                 irods::file_object_ptr file_obj = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
                 status = putTheFile(
                              wos_host, 
                              wos_policy, 
                              (const char *)_cache_file_name, 
                              file_obj->physical_path().c_str(),
                              _ctx,
                              &theHeaders);
                 // returns non-zero on error.
                 if (!status) {
                    // We want to set the new physical path
                    if( theHeaders.x_ddn_oid && strlen( theHeaders.x_ddn_oid ) > 0 ) {
                        file_obj->physical_path(std::string(theHeaders.x_ddn_oid));
                    } else {
                        result = ERROR( status, "wosSyncToArchPlugin - OID string is empty");
                    }

                 } else {
                     result =  ERROR( status, "wosSyncToArchPlugin - error in putTheFile");
                 }
              }
           }
        }

        return result;

    } // wosSyncToArchPlugin

    // =-=-=-=-=-=-=-
    // redirect_get - code to determine redirection for get operation
    irods::error wosRedirectCreate( 
                      irods::plugin_property_map& _prop_map,
                      irods::file_object_ptr        _file_obj,
                      const std::string&             _resc_name, 
                      const std::string&             _curr_host, 
                      float&                         _out_vote ) {

        irods::error result = SUCCESS();

        // =-=-=-=-=-=-=-
        // determine if the resource is down 
        int resc_status = 0;

        irods::error get_ret = _prop_map.get< int >( irods::RESOURCE_STATUS, resc_status );
        if((result = ASSERT_PASS(get_ret, "wosRedirectCreate - failed to get 'status' property")).ok()) {

           // =-=-=-=-=-=-=-
           // if the status is down, vote no.
           if( INT_RESC_STATUS_DOWN == resc_status ) {
               _out_vote = 0.0;
           } else {
   
              // =-=-=-=-=-=-=-
              // get the resource host for comparison to curr host
              std::string host_name;
              get_ret = _prop_map.get< std::string >( irods::RESOURCE_LOCATION, host_name );
              if((result = ASSERT_PASS(get_ret, "wosRedirectCreate - failed to get 'location' prop")).ok()) {
                 // vote higher if we are on the same host
                 if( _curr_host == host_name ) {
                     _out_vote = 1.0;
                 } else {
                     _out_vote = 0.5;
                 }
              }
           }
        }
        return result;

    } // wosRedirectCreate

    /// =-=-=-=-=-=-=-
    /// @brief given a property map and file object, attempt to fetch it from 
    ///        the WOS system as it may be replicated under the covers.  we then
    ///        regsiter the archive version as a proper replica
    irods::error register_archive_object(
        irods::plugin_context&  _ctx,
        irods::file_object_ptr           _file_obj ) {
        // =-=-=-=-=-=-=-
        // get the name of this resource
        std::string resc_name;
        irods::error ret = _ctx.prop_map().get< std::string >( irods::RESOURCE_NAME, resc_name );
        if( !ret.ok() ) {
            return PASS( ret );
        }

        // =-=-=-=-=-=-=-
        // first scan for a repl with this resource in the
        // hierarchy, if there is one then no need to continue
        // =-=-=-=-=-=-=-
        std::vector< irods::physical_object > objs = _file_obj->replicas();
        std::vector< irods::physical_object >::iterator itr = objs.begin();
        for ( ; itr != objs.end(); ++itr ) {
            if( std::string::npos != itr->resc_hier().find( resc_name ) ) {
                return SUCCESS();
            }

        } // for itr

        // =-=-=-=-=-=-=-
        // get the repl policy to determine if we need to check for an archived
        // replica and if so register it
        std::string repl_policy;
        ret = _ctx.prop_map().get< std::string >( REPL_POLICY_KEY, repl_policy );
        if( !ret.ok() ) {
            return ERROR( INVALID_OBJECT_NAME, "object not found on the archive" );
        }

        // =-=-=-=-=-=-=-
        // check the repl policy
        // TODO

        // =-=-=-=-=-=-=-
        // search for a phypath with NO separator, this should be the object id
        std::string obj_id;
        std::string virt_sep = irods::get_virtual_path_separator();
        for ( itr  = objs.begin();
              itr != objs.end(); 
              ++itr ) {

            size_t pos = itr->path().find( virt_sep );
            if( std::string::npos == pos ) {
                obj_id = itr->path();
                break;
            } else {
                rodsLog(LOG_DEBUG6, "%s - [%s] is not oid", __FUNCTION__, itr->path().c_str());
            }
        }

        // TODO see of obj_id is not found
        if (obj_id.empty()) {
            return ERROR(INVALID_OBJECT_NAME, "obj_id is empty");
        }

        // =-=-=-=-=-=-=-
        // get the wos host property
        std::string wos_host;
        ret = _ctx.prop_map().get< std::string >( WOS_HOST_KEY, wos_host );
        if( !ret.ok() ) {
            return PASS( ret );
        }

        // =-=-=-=-=-=-=-
        // perform a stat on the obj id to see if it is there
        WOS_HEADERS wos_headers;
        memset( &wos_headers, 0, sizeof( wos_headers ) );
        int status = getTheFileStatus( wos_host.c_str(), obj_id.c_str(), &wos_headers );
        if ( status ) {
            return ERROR( status, "error in getTheFileStatus");

        } 

        // =-=-=-=-=-=-=-
        // get our parent resource
        rodsLong_t resc_id = 0;
        ret = _ctx.prop_map().get<rodsLong_t>( irods::RESOURCE_ID, resc_id );
        if( !ret.ok() ) {
            return PASS( ret );
        }

        std::string resc_hier;
        ret = resc_mgr.leaf_id_to_hier(resc_id, resc_hier);
        if( !ret.ok() ) {
            return PASS( ret );
        }

        // =-=-=-=-=-=-=-
        // get the root resc of the hier
        std::string root_resc;
        irods::hierarchy_parser parser;
        parser.set_string( resc_hier );
        parser.first_resc( root_resc );
        
        // =-=-=-=-=-=-=-
        // find the highest repl number for this data object
        int max_repl_num = 0;
        for ( itr  = objs.begin();
              itr != objs.end(); 
              ++itr ) {
            if( itr->repl_num() > max_repl_num ) {
                max_repl_num = itr->repl_num();
            
            }

        } // for itr

        // =-=-=-=-=-=-=-
        // grab the first physical object to reference
        // for the various properties in the obj info
        // physical object to mine for various properties
        itr = objs.begin();

        // =-=-=-=-=-=-=-
        // build out a dataObjInfo_t struct for use in the call
        // to rsRegDataObj
        dataObjInfo_t dst_data_obj;
        bzero( &dst_data_obj, sizeof( dst_data_obj ) );

        resc_mgr.hier_to_leaf_id(resc_hier.c_str(), dst_data_obj.rescId);
        strncpy( dst_data_obj.objPath,       itr->name().c_str(),             MAX_NAME_LEN );
        strncpy( dst_data_obj.rescName,      root_resc.c_str(),               NAME_LEN );
        strncpy( dst_data_obj.rescHier,      resc_hier.c_str(),               MAX_NAME_LEN );
        strncpy( dst_data_obj.dataType,      itr->type_name( ).c_str(),       NAME_LEN );
        dst_data_obj.dataSize = itr->size( );
        strncpy( dst_data_obj.chksum,        itr->checksum( ).c_str(),        NAME_LEN );
        strncpy( dst_data_obj.version,       itr->version( ).c_str(),         NAME_LEN );
        strncpy( dst_data_obj.filePath,      obj_id.c_str(),                  MAX_NAME_LEN );
        strncpy( dst_data_obj.dataOwnerName, itr->owner_name( ).c_str(),      NAME_LEN );
        strncpy( dst_data_obj.dataOwnerZone, itr->owner_zone( ).c_str(),      NAME_LEN );
        dst_data_obj.replNum    = max_repl_num+1;
        dst_data_obj.replStatus = itr->is_dirty( );
        strncpy( dst_data_obj.statusString,  itr->status( ).c_str(),          NAME_LEN );
        dst_data_obj.dataId = itr->id(); 
        dst_data_obj.collId = itr->coll_id(); 
        dst_data_obj.dataMapId = 0; 
        dst_data_obj.flags     = 0; 
        strncpy( dst_data_obj.dataComments,  itr->r_comment( ).c_str(),       MAX_NAME_LEN );
        strncpy( dst_data_obj.dataMode,      itr->mode( ).c_str(),            SHORT_STR_LEN );
        strncpy( dst_data_obj.dataExpiry,    itr->expiry_ts( ).c_str(),       TIME_LEN );
        strncpy( dst_data_obj.dataCreate,    itr->create_ts( ).c_str(),       TIME_LEN );
        strncpy( dst_data_obj.dataModify,    itr->modify_ts( ).c_str(),       TIME_LEN );

        // =-=-=-=-=-=-=-
        // manufacture a src data obj
        dataObjInfo_t src_data_obj;
        memcpy( &src_data_obj, &dst_data_obj, sizeof( dst_data_obj ) );
        src_data_obj.replNum = itr->repl_num();
        strncpy( src_data_obj.filePath, itr->path().c_str(),       MAX_NAME_LEN );
        strncpy( src_data_obj.rescHier, itr->resc_hier().c_str(),  MAX_NAME_LEN );

        // =-=-=-=-=-=-=-
        // repl to an existing copy
        regReplica_t reg_inp;
        bzero( &reg_inp, sizeof( reg_inp ) );
        reg_inp.srcDataObjInfo  = &src_data_obj;
        reg_inp.destDataObjInfo = &dst_data_obj;
        status = rsRegReplica( _ctx.comm(), &reg_inp );
        if( status < 0 ) {
            return ERROR( status, "failed to register data object" );
        }
        
        // =-=-=-=-=-=-=-
        // we need to make a physical object and add it to the file_object
        // so it can get picked up for the repl operation
        irods::physical_object phy_obj = (*itr);
        phy_obj.resc_hier( dst_data_obj.rescHier );
        phy_obj.repl_num( dst_data_obj.replNum );
        objs.push_back( phy_obj );
        _file_obj->replicas( objs );

        // =-=-=-=-=-=-=-
        // repave resc hier in file object as it is
        // what is used to determine hierarchy in
        // the compound resource
        _file_obj->resc_hier( dst_data_obj.rescHier );
        _file_obj->physical_path( dst_data_obj.filePath );

        return SUCCESS();

    } // register_archive_object
    
    // =-=-=-=-=-=-=-
    // redirect_get - code to determine redirection for get operation
    irods::error wosRedirectOpen( 
        irods::plugin_context&  _ctx,
        irods::plugin_property_map& _prop_map,
        irods::file_object_ptr      _file_obj,
        const std::string&          _resc_name, 
        const std::string&          _curr_host, 
        float&                      _out_vote ) {
        irods::error result = SUCCESS();

        // =-=-=-=-=-=-=-
        // determine if the resource is down 
        int resc_status = 0;
        irods::error get_ret = _prop_map.get< int >( irods::RESOURCE_STATUS, resc_status );
        if((result = ASSERT_PASS(get_ret, "wosRedirectOpen - failed to get 'status' property")).ok()) {

           // =-=-=-=-=-=-=-
           // if the status is down, vote no.
           if( INT_RESC_STATUS_DOWN == resc_status ) {
               _out_vote = 0.0;
           } else {
   
              // =-=-=-=-=-=-=-
              // get the resource host for comparison to curr host
              std::string host_name;
              get_ret = _prop_map.get< std::string >( irods::RESOURCE_LOCATION, host_name );
              if((result = ASSERT_PASS(get_ret, "wosRedirectOpen - failed to get 'location' prop")).ok()) {
                 // =-=-=-=-=-=-=-
                 
                 // consider registration of object on WOS if it is not already but only if we are on the current host
                 if ( _curr_host == host_name ) {
                     get_ret = register_archive_object( 
                                       _ctx,
                                       _file_obj );

                     if((result = ASSERT_PASS(get_ret, "wosRedirectOpen - register_archive_object failed")).ok()) {
                         // =-=-=-=-=-=-=-
                         // vote higher if we are on the same host
                         if( _curr_host == host_name ) {
                             _out_vote = 1.0;
                         } //else {
                         //    _out_vote = 0.5;
                         //}
                     }
                 
                 }
              }
           }
        } 
        return result;

    } // wosRedirectOpen

    // =-=-=-=-=-=-=-
    // used to allow the resource to determine which host
    // should provide the requested operation
    irods::error wosRedirectPlugin( 
        irods::plugin_context&  _ctx,
        const std::string*                _opr,
        const std::string*                _curr_host,
        irods::hierarchy_parser*         _out_parser,
        float*                            _out_vote ) {

        irods::error result = SUCCESS();

        // =-=-=-=-=-=-=-
        // check the context validity
        irods::error ret = _ctx.valid< irods::file_object >(); 
        if((result = ASSERT_PASS(ret, "Invalid parameters or physical path.")).ok()) {
    
           // =-=-=-=-=-=-=-
           // check incoming parameters
           if( !_opr  ) {
               result =  ERROR( -1, "wosRedirectPlugin - null operation" );
           } else if( !_curr_host ) {
               result =  ERROR( -1, "wosRedirectPlugin - null current host" );
           } else  if( !_out_parser ) {
               result =  ERROR( -1, "wosRedirectPlugin - null outgoing hier parser" );
           } else if( !_out_vote ) {
               result =  ERROR( -1, "wosRedirectPlugin - null outgoing vote" );
           } else {
              // =-=-=-=-=-=-=-
              // cast down the chain to our understood object type
              irods::file_object_ptr file_obj = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
      
              // =-=-=-=-=-=-=-
              // get the name of this resource
              std::string resc_name;
              ret = _ctx.prop_map().get< std::string >( irods::RESOURCE_NAME, resc_name );
              if((result = ASSERT_PASS(ret, "wosRedirectPlugin - failed in get property for name")).ok()) {
                 // =-=-=-=-=-=-=-
                 // add ourselves to the hierarchy parser by default
                 _out_parser->add_child( resc_name );
         
                 // =-=-=-=-=-=-=-
                 // test the operation to determine which choices to make
                 if( irods::OPEN_OPERATION == (*_opr) ) {
                     // =-=-=-=-=-=-=-
                     // call redirect determination for 'get' operation
                     result =  
                        wosRedirectOpen( _ctx, _ctx.prop_map(), file_obj, resc_name, (*_curr_host), (*_out_vote));
                 } else if( irods::CREATE_OPERATION == (*_opr) ) {
                     // =-=-=-=-=-=-=-
                     // call redirect determination for 'create' operation
                     result =  
                        wosRedirectCreate( _ctx.prop_map(), file_obj, resc_name, (*_curr_host), (*_out_vote));
                 } else {
                   result = ERROR(-1, "wosRedirectPlugin - operation not supported");
                 }
              }
           }
        } 
        return result;

    } // wosRedirectPlugin

    class wos_resource : public irods::resource {
    public:
        wos_resource( const std::string& _inst_name,
                const std::string& _context ) :
            irods::resource( _inst_name, _context ) {
            // =-=-=-=-=-=-=-
            // parse context string into property pairs assuming a ; as a separator
            std::vector< std::string > props;
            rodsLog( LOG_DEBUG, "wos context: %s", _context.c_str());
            irods::kvp_map_t kvp;
            irods::parse_kvp_string(
                _context,
                kvp );

            // =-=-=-=-=-=-=-
            // copy the properties from the context to the prop map
            irods::kvp_map_t::iterator itr = kvp.begin();
            for( ; itr != kvp.end(); ++itr ) {
                properties_.set< std::string >( 
                    itr->first,
                    itr->second );
                    
            } // for itr

            // =-=-=-=-=-=-=-
            // check for certain properties
            std::string my_host;
            irods::error prop_ret = properties_.get< std::string >( WOS_HOST_KEY, my_host );
            if (!prop_ret.ok()) {
                std::stringstream msg;
                rodsLog( LOG_ERROR, "prop_map has no wos_host " );
            }

            std::string retry_str; 
            prop_ret = properties_.get< std::string >( NUM_RETRIES_KEY, retry_str );
            if( prop_ret.ok() ) {
                size_t retry_sz = 0;
                try {
                    retry_sz = boost::lexical_cast< size_t >( retry_str );
                    if( retry_sz <= MAX_RETRY_COUNT ) { 
                        RETRY_COUNT = retry_sz;
                    } else {
                        rodsLog( 
                            LOG_ERROR, 
                            "wos_resource - retry count %ld, exceeded max %ld",
                            retry_sz,
                            MAX_RETRY_COUNT );
                    }

                } catch( boost::bad_lexical_cast e ) {
                        rodsLog( 
                            LOG_ERROR,
                            "wos_resource - failed to lexical cast [%s] to size_t",
                            retry_str.c_str() );
                }

            }

            std::string timeout_str;
            prop_ret = properties_.get< std::string >( CONNECT_TIMEOUT_KEY, timeout_str );
            if( prop_ret.ok() ) {
                size_t timeout_sz = 0;
                try {
                    timeout_sz = boost::lexical_cast< size_t >( timeout_str );
                    if( timeout_sz <= MAX_CONNECT_TIMEOUT ) {
                        CONNECT_TIMEOUT = timeout_sz;
                    } else {
                        rodsLog(
                            LOG_ERROR,
                            "wos_resource - connect timeout %ld, exceeded max %ld",
                            timeout_sz,
                            MAX_CONNECT_TIMEOUT );
                    }

                } catch( boost::bad_lexical_cast e ) {
                        rodsLog(
                            LOG_ERROR,
                            "wos_resource - failed to lexical cast [%s] to size_t",
                            timeout_str.c_str() );
                }

            }

        } // ctor

        irods::error need_post_disconnect_maintenance_operation( bool& _b ) {
            _b = false;
            return SUCCESS();
        }


        // =-=-=-=-=-=-=-
        // 3b. pass along a functor for maintenance work after
        //     the client disconnects, uncomment the first two lines for effect.
        irods::error post_disconnect_maintenance_operation( irods::pdmo_type& _op  ) {
            return SUCCESS();
        }


        ~wos_resource() {
        }

    }; // class wos_resource

    // =-=-=-=-=-=-=-
    // Create the plugin factory function which will return a microservice
    // table entry containing the microservice function pointer, the number
    // of parameters that the microservice takes and the name of the micro
    // service.  this will be called by the plugin loader in the irods server
    // to create the entry to the table when the plugin is requested.
    extern "C"
    irods::resource* plugin_factory(const std::string& _inst_name, const std::string& _context) {
        wos_resource* resc = new wos_resource( _inst_name, _context );
        using namespace irods;
        using namespace std;
        resc->add_operation(
            RESOURCE_OP_CREATE,
            function<error(plugin_context&)>(
                wosFileCreatePlugin));
        resc->add_operation(
            irods::RESOURCE_OP_OPEN,
            function<error(plugin_context&)>(
                wosFileOpenPlugin));
        resc->add_operation<void*,int>(
            irods::RESOURCE_OP_READ,
            std::function<
                error(irods::plugin_context&,void*,int)>(
                    wosFileReadPlugin));
        resc->add_operation<void*,int>(
            irods::RESOURCE_OP_WRITE,
            function<error(plugin_context&,void*,int)>(
                wosFileWritePlugin));
        resc->add_operation(
            RESOURCE_OP_CLOSE,
            function<error(plugin_context&)>(
                wosFileClosePlugin));
        resc->add_operation(
            irods::RESOURCE_OP_UNLINK,
            function<error(plugin_context&)>(
                wosFileUnlinkPlugin));
        resc->add_operation<struct stat*>(
            irods::RESOURCE_OP_STAT,
            function<error(plugin_context&, struct stat*)>(
                wosFileStatPlugin));
        resc->add_operation(
            irods::RESOURCE_OP_MKDIR,
            function<error(plugin_context&)>(
                wosFileMkdirPlugin));
        resc->add_operation(
            irods::RESOURCE_OP_OPENDIR,
            function<error(plugin_context&)>(
                wosFileOpendirPlugin));
        resc->add_operation<struct rodsDirent**>(
            irods::RESOURCE_OP_READDIR,
            function<error(plugin_context&,struct rodsDirent**)>(
                wosFileReaddirPlugin));
        resc->add_operation<const char*>(
            irods::RESOURCE_OP_RENAME,
            function<error(plugin_context&, const char*)>(
                wosFileRenamePlugin));
        resc->add_operation(
            irods::RESOURCE_OP_FREESPACE,
            function<error(plugin_context&)>(
                wosFileGetFsFreeSpacePlugin));
        resc->add_operation<long long, int>(
            irods::RESOURCE_OP_LSEEK,
            function<error(plugin_context&, long long, int)>(
                wosFileLseekPlugin));
        resc->add_operation(
            irods::RESOURCE_OP_CLOSEDIR,
            function<error(plugin_context&)>(
                wosFileClosedirPlugin));
        resc->add_operation<const char*>(
            irods::RESOURCE_OP_STAGETOCACHE,
            function<error(plugin_context&, const char*)>(
                wosStageToCachePlugin));
        resc->add_operation<const char*>(
            irods::RESOURCE_OP_SYNCTOARCH,
            function<error(plugin_context&, const char*)>(
                wosSyncToArchPlugin));
        resc->add_operation(
            irods::RESOURCE_OP_REGISTERED,
            function<error(plugin_context&)>(
                wosRegisteredPlugin));
        resc->add_operation(
            irods::RESOURCE_OP_UNREGISTERED,
            function<error(plugin_context&)>(
                wosUnregisteredPlugin));
        resc->add_operation(
            irods::RESOURCE_OP_MODIFIED,
            function<error(plugin_context&)>(
                wosModifiedPlugin));
        resc->add_operation(
            irods::RESOURCE_OP_RMDIR,
            function<error(plugin_context&)>(
                wosFileRmdirPlugin));
        resc->add_operation<const std::string*, const std::string*, irods::hierarchy_parser*, float*>(
            irods::RESOURCE_OP_RESOLVE_RESC_HIER,
            function<error(plugin_context&,const std::string*, const std::string*, irods::hierarchy_parser*, float*)>(
                wosRedirectPlugin));

        // set some properties necessary for backporting to iRODS legacy code
        resc->set_property< int >( irods::RESOURCE_CHECK_PATH_PERM, DO_CHK_PATH_PERM );
        resc->set_property< int >( irods::RESOURCE_CREATE_PATH,     CREATE_PATH );
        return dynamic_cast<irods::resource *>( resc );

    } // plugin_factory



