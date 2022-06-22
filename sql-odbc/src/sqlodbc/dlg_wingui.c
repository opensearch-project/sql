#ifdef WIN32

#include "dlg_specific.h"
#include "opensearch_apifunc.h"
#include "loadlib.h"
#include "misc.h"  // strncpy_null
#include "win_setup.h"
#ifdef _HANDLE_ENLIST_IN_DTC_
#include "connexp.h"
#include "xalibname.h"
#endif /* _HANDLE_ENLIST_IN_DTC_ */

#define HTTP_PREFIX   "http://"
#define HTTPS_PREFIX  "https://"

#define AUTHMODE_CNT 3
#define LOGLEVEL_CNT 8
extern HINSTANCE s_hModule;

int loglevels[LOGLEVEL_CNT] = {
    {IDS_LOGTYPE_OFF},
    {IDS_LOGTYPE_FATAL},
    {IDS_LOGTYPE_ERROR},
    {IDS_LOGTYPE_WARNING},
    {IDS_LOGTYPE_INFO},
    {IDS_LOGTYPE_DEBUG}, 
    {IDS_LOGTYPE_TRACE},
    {IDS_LOGTYPE_ALL}};

static const struct authmode authmodes[AUTHMODE_CNT] = {
    {IDS_AUTHTYPE_NONE, AUTHTYPE_IAM},
    {IDS_AUTHTYPE_BASIC, AUTHTYPE_BASIC},
    {IDS_AUTHTYPE_IAM, AUTHTYPE_NONE}};

const struct authmode *GetCurrentAuthMode(HWND hdlg) {
    unsigned int ams_cnt = 0;
    const struct authmode *ams = GetAuthModes(&ams_cnt);
    unsigned int authtype_selection_idx = (unsigned int)(DWORD)SendMessage(
        GetDlgItem(hdlg, IDC_AUTHTYPE), CB_GETCURSEL, 0L, 0L);
    if (authtype_selection_idx >= ams_cnt)
        authtype_selection_idx = 0;
    return &ams[authtype_selection_idx];
}

int *GetLogLevels(unsigned int *count) {
    *count = LOGLEVEL_CNT;
    return loglevels;
}

int GetCurrentLogLevel(HWND hdlg) {
    unsigned int log_cnt = 0;
    int *log = GetLogLevels(&log_cnt);
    unsigned int loglevel_selection_idx = (unsigned int)(DWORD)SendMessage(
        GetDlgItem(hdlg, IDC_LOG_LEVEL), CB_GETCURSEL, 0L, 0L);
    if (loglevel_selection_idx >= log_cnt)
        loglevel_selection_idx = 0;
    return log[loglevel_selection_idx];
}


void SetAuthenticationVisibility(HWND hdlg, const struct authmode *am) {
    if (strcmp(am->authtype_str, AUTHTYPE_BASIC) == 0) {
        EnableWindow(GetDlgItem(hdlg, IDC_USER), TRUE);
        EnableWindow(GetDlgItem(hdlg, IDC_PASSWORD), TRUE);
        EnableWindow(GetDlgItem(hdlg, IDC_REGION), FALSE);
    } else if (strcmp(am->authtype_str, AUTHTYPE_IAM) == 0) {
        EnableWindow(GetDlgItem(hdlg, IDC_USER), FALSE);
        EnableWindow(GetDlgItem(hdlg, IDC_PASSWORD), FALSE);
        EnableWindow(GetDlgItem(hdlg, IDC_REGION), TRUE);
    } else {
        EnableWindow(GetDlgItem(hdlg, IDC_USER), FALSE);
        EnableWindow(GetDlgItem(hdlg, IDC_PASSWORD), FALSE);
        EnableWindow(GetDlgItem(hdlg, IDC_REGION), FALSE);
    } 
}

void SetDlgStuff(HWND hdlg, const ConnInfo *ci) {
    // Connection
    SetDlgItemText(hdlg, IDC_DRIVER_VERSION, "V."OPENSEARCHDRIVERVERSION);
    SetDlgItemText(hdlg, IDC_DSNAME, ci->dsn);
    SetDlgItemText(hdlg, IDC_SERVER, ci->server);
    SetDlgItemText(hdlg, IDC_PORT, ci->port);

    // Authentication
    int authtype_selection_idx = 0;
    unsigned int ams_cnt = 0;
    const struct authmode *ams = GetAuthModes(&ams_cnt);
    char buff[MEDIUM_REGISTRY_LEN + 1];
    for (unsigned int i = 0; i < ams_cnt; i++) {
        LoadString(GetWindowInstance(hdlg), ams[i].authtype_id, buff,
                   MEDIUM_REGISTRY_LEN);
        SendDlgItemMessage(hdlg, IDC_AUTHTYPE, CB_ADDSTRING, 0, (WPARAM)buff);
        if (!stricmp(ci->authtype, ams[i].authtype_str)) {
            authtype_selection_idx = i;
        }
    }
    SendDlgItemMessage(hdlg, IDC_AUTHTYPE, CB_SETCURSEL,
                       ams[authtype_selection_idx].authtype_id, (WPARAM)0);
    SetDlgItemText(hdlg, IDC_USER, ci->username);
    SetDlgItemText(hdlg, IDC_PASSWORD, SAFE_NAME(ci->password));
    SetDlgItemText(hdlg, IDC_REGION, ci->region);
}

static void GetNameField(HWND hdlg, int item, opensearchNAME *name) {
    char medium_buf[MEDIUM_REGISTRY_LEN + 1];
    GetDlgItemText(hdlg, item, medium_buf, sizeof(medium_buf));
    STR_TO_NAME((*name), medium_buf);
}

void GetDlgStuff(HWND hdlg, ConnInfo *ci) {
    // Connection
    GetDlgItemText(hdlg, IDC_DESC, ci->desc, sizeof(ci->desc));
    GetDlgItemText(hdlg, IDC_SERVER, ci->server, sizeof(ci->server));
    GetDlgItemText(hdlg, IDC_PORT, ci->port, sizeof(ci->port));

    // Authentication
    GetDlgItemText(hdlg, IDC_USER, ci->username, sizeof(ci->username));
    GetNameField(hdlg, IDC_PASSWORD, &ci->password);
    GetDlgItemText(hdlg, IDC_REGION, ci->region, sizeof(ci->region));
    const struct authmode *am = GetCurrentAuthMode(hdlg);
    SetAuthenticationVisibility(hdlg, am);
    STRCPY_FIXED(ci->authtype, am->authtype_str);

}

const struct authmode *GetAuthModes(unsigned int *count) {
    *count = AUTHMODE_CNT;
    return authmodes;
}
static void getDriversDefaultsOfCi(const ConnInfo *ci, GLOBAL_VALUES *glbv) {
    const char *drivername = NULL;

    if (ci->drivername[0])
        drivername = ci->drivername;
    else if (NAME_IS_VALID(ci->drivers.drivername))
        drivername = SAFE_NAME(ci->drivers.drivername);
    if (drivername && drivername[0])
        getDriversDefaults(drivername, glbv);
    else
        getDriversDefaults(INVALID_DRIVER, glbv);
}

/**
 * @brief Initializes and closes the advanced dialog box.
 * 
 * @param hdlg : Handle to dialog box
 * @param wMsg : Dialog box command message
 * @param wParam : Handle to the control to receive keyboard focus
 * @param lParam : Dialog connection data
 * @return INT_PTR : Returns true on successful command of advanced dialog box
 */
INT_PTR CALLBACK advancedOptionsProc(HWND hdlg, UINT wMsg, WPARAM wParam,
                                     LPARAM lParam) {
    switch (wMsg) {
        case WM_INITDIALOG: {
            SetWindowLongPtr(hdlg, DWLP_USER, lParam);
            ConnInfo *ci = (ConnInfo *)lParam;

            // To avoid cases in which the "UseSSL" flag is different from a specified server protocol
            if (strncmp(HTTP_PREFIX, ci->server, strlen(HTTP_PREFIX)) == 0) {
                CheckDlgButton(hdlg, IDC_USESSL, FALSE);
                CheckDlgButton(hdlg, IDC_HOST_VER, FALSE);
                EnableWindow(GetDlgItem(hdlg, IDC_USESSL), FALSE);
                EnableWindow(GetDlgItem(hdlg, IDC_HOST_VER), FALSE);
            } else if (strncmp(HTTPS_PREFIX, ci->server, strlen(HTTPS_PREFIX)) == 0) {
                CheckDlgButton(hdlg, IDC_USESSL, TRUE);
                CheckDlgButton(hdlg, IDC_HOST_VER, ci->verify_server);
                EnableWindow(GetDlgItem(hdlg, IDC_USESSL), FALSE);
            } else {
                CheckDlgButton(hdlg, IDC_USESSL, ci->use_ssl);
                CheckDlgButton(hdlg, IDC_HOST_VER, ci->verify_server);
            }

            SetDlgItemText(hdlg, IDC_CONNTIMEOUT, ci->response_timeout);
            SetDlgItemText(hdlg, IDC_FETCH_SIZE, ci->fetch_size);
            break;
        }

        case WM_COMMAND: {
            ConnInfo *ci = (ConnInfo *)GetWindowLongPtr(hdlg, DWLP_USER);
            switch (GET_WM_COMMAND_ID(wParam, lParam)) {
                case IDOK:
                    // Get Dialog Values 
                    ci->use_ssl = (IsDlgButtonChecked(hdlg, IDC_USESSL) ? 1 : 0);
                    ci->verify_server = (IsDlgButtonChecked(hdlg, IDC_HOST_VER) ? 1 : 0);
                    GetDlgItemText(hdlg, IDC_CONNTIMEOUT, ci->response_timeout,
                                   sizeof(ci->response_timeout));
                    GetDlgItemText(hdlg, IDC_FETCH_SIZE, ci->fetch_size,
                                   sizeof(ci->fetch_size));
                case IDCANCEL:
                    EndDialog(hdlg, FALSE);
                    return TRUE;
            }
        }
    }
    return FALSE;
}

INT_PTR CALLBACK logOptionsProc(HWND hdlg, UINT wMsg, WPARAM wParam,
                                LPARAM lParam) {
    switch (wMsg) {
        case WM_INITDIALOG: {
            ConnInfo *ci = (ConnInfo *)lParam;
            SetWindowLongPtr(hdlg, DWLP_USER, lParam);

            // Logging
            int loglevel_selection_idx = 0;
            unsigned int log_cnt = 0;
            int *log = GetLogLevels(&log_cnt);
            char buff[MEDIUM_REGISTRY_LEN + 1];
            for (unsigned int i = 0; i < log_cnt; i++) {
                LoadString(GetWindowInstance(hdlg), log[i], buff,
                           MEDIUM_REGISTRY_LEN);
                SendDlgItemMessage(hdlg, IDC_LOG_LEVEL, CB_ADDSTRING, 0,
                                   (WPARAM)buff);
                if ((unsigned int)ci->drivers.loglevel == i) {
                    loglevel_selection_idx = i;
                }
            }
            SendDlgItemMessage(hdlg, IDC_LOG_LEVEL, CB_SETCURSEL,
                               loglevel_selection_idx, (WPARAM)0);
            SetDlgItemText(hdlg, IDC_LOG_PATH, ci->drivers.output_dir);
            break;
        }

        case WM_COMMAND: {
            ConnInfo *ci = (ConnInfo *)GetWindowLongPtr(hdlg, DWLP_USER);
            switch (GET_WM_COMMAND_ID(wParam, lParam)) {
                case IDOK: {
                    // Get Dialog Values
                    int log = GetCurrentLogLevel(hdlg);
                    switch (log) {
                        case IDS_LOGTYPE_OFF:
                            ci->drivers.loglevel = (char)OPENSEARCH_OFF;
                            break;
                        case IDS_LOGTYPE_FATAL:
                            ci->drivers.loglevel = (char)OPENSEARCH_FATAL;
                            break;
                        case IDS_LOGTYPE_ERROR:
                            ci->drivers.loglevel = (char)OPENSEARCH_ERROR;
                            break;
                        case IDS_LOGTYPE_WARNING:
                            ci->drivers.loglevel = (char)OPENSEARCH_WARNING;
                            break;
                        case IDS_LOGTYPE_INFO:
                            ci->drivers.loglevel = (char)OPENSEARCH_INFO;
                            break;
                        case IDS_LOGTYPE_DEBUG:
                            ci->drivers.loglevel = (char)OPENSEARCH_DEBUG;
                            break;
                        case IDS_LOGTYPE_TRACE:
                            ci->drivers.loglevel = (char)OPENSEARCH_TRACE;
                            break;
                        case IDS_LOGTYPE_ALL:
                            ci->drivers.loglevel = (char)OPENSEARCH_ALL;
                            break;
                        default:
                            ci->drivers.loglevel = (char)OPENSEARCH_OFF;
                            break;
                    }
                    setGlobalCommlog(ci->drivers.loglevel);
                    setGlobalDebug(ci->drivers.loglevel);
                    writeGlobalLogs();
                    GetDlgItemText(hdlg, IDC_LOG_PATH, ci->drivers.output_dir,
                                   sizeof(ci->drivers.output_dir));
                    setLogDir(ci->drivers.output_dir);
                }

                case IDCANCEL:
                    EndDialog(hdlg, FALSE);
                    return TRUE;
                }
            }
    }
    return FALSE;
}

#endif /* WIN32 */
